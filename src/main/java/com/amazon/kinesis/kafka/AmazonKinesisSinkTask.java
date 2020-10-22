package com.amazon.kinesis.kafka;

import java.util.*;
import java.util.concurrent.*;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.util.StringUtils;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmazonKinesisSinkTask extends SinkTask {

    private String streamName;

    private String regionName;

    private String roleARN;

    private String baseRegionName;

    private String baseRoleARN;

    private String roleExternalID;

    private String roleSessionName;

    private int roleDurationSeconds;

    private String kinesisEndpoint;

    private int maxConnections;

    private int rateLimit;

    private int maxBufferedTime;

    private int ttl;

    private String metricsLevel;

    private String metricsGranuality;

    private String metricsNameSpace;

    private boolean aggregration;

    private boolean usePartitionAsHashKey;

    private boolean flushSync;

    private boolean singleKinesisProducerPerPartition;

    private boolean pauseConsumption;

    private int outstandingRecordsThreshold;

    private int sleepPeriod;

    private int sleepCycles;

    private SinkTaskContext sinkTaskContext;

    private Map<String, KinesisProducer> producerMap = new HashMap<String, KinesisProducer>();

    private KinesisProducer kinesisProducer;

    private static final Logger log = LoggerFactory.getLogger(AmazonKinesisSinkTask.class);

    @Override
    public void initialize(SinkTaskContext context) {
        sinkTaskContext = context;
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) {
        // TODO Auto-generated method stub
        if (singleKinesisProducerPerPartition) {
            producerMap.values().forEach(producer -> {
                if (flushSync)
                    producer.flushSync();
                else
                    producer.flush();
            });
        } else {
            if (flushSync)
                kinesisProducer.flushSync();
            else
                kinesisProducer.flush();
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {

        // If KinesisProducers cannot write to Kinesis Streams (because of
        // connectivity issues, access issues
        // or misconfigured shards we will pause consumption of messages till
        // backlog is cleared

        CountDownLatch atLeastOneWritten = new CountDownLatch(sinkRecords.size() > 0 ? 1 : 0);
        List<ListenableFuture<UserRecordResult>> submittedFutures = new ArrayList<>();
        validateOutStandingRecords();

        String partitionKey;
        for (SinkRecord sinkRecord : sinkRecords) {

            ListenableFuture<UserRecordResult> f;
            // Kinesis does not allow empty partition key
            if (sinkRecord.key() != null && !sinkRecord.key().toString().trim().equals("")) {
                partitionKey = sinkRecord.key().toString().trim();
            } else {
                partitionKey = Integer.toString(sinkRecord.kafkaPartition());
            }

            if (singleKinesisProducerPerPartition)
                f = addUserRecord(producerMap.get(sinkRecord.kafkaPartition() + "@" + sinkRecord.topic()), streamName,
                        partitionKey, usePartitionAsHashKey, sinkRecord);
            else
                f = addUserRecord(kinesisProducer, streamName, partitionKey, usePartitionAsHashKey, sinkRecord);

            f.addListener(() -> {
                atLeastOneWritten.countDown();
            }, MoreExecutors.directExecutor());
            submittedFutures.add(f);
        }
        waitForAtLeastOne(atLeastOneWritten, submittedFutures);
    }

    private void waitForAtLeastOne(CountDownLatch atLeastOneWritten, List<ListenableFuture<UserRecordResult>> submittedFutures) {
        try {
            try {
                atLeastOneWritten.await(this.ttl, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("Interrupted waiting for the first result");
                throw new ConnectException("Interrupted waiting for first result", e);
            }
            if (submittedFutures.isEmpty()) {
                log.info("No submitted futures found");
            } else {
                ListenableFuture<UserRecordResult> waitingFuture = submittedFutures.iterator().next();
                UserRecordResult result = waitingFuture.get(this.maxBufferedTime + this.ttl, TimeUnit.MILLISECONDS);
                if (!result.isSuccessful()) {
                    throwErrorFromAttempts(result);
                }
            }
        } catch (ExecutionException|InterruptedException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof UserRecordFailedException) {
                throwErrorFromAttempts(((UserRecordFailedException)ex.getCause()).getResult());
            }
            throw new ConnectException("Producer failed" + ex.getMessage(), ex);
        } catch (TimeoutException ex) {
            throw new ConnectException("Kinesis Producer failed to publish data in a reasonable time interval and timed out.");
        }
    }

    private void throwErrorFromAttempts(UserRecordResult result) {
        Attempt last = Iterables.getLast(
                Iterables.filter(result.getAttempts(), r -> !"Expired".equals(r.getErrorCode())), // first not expired
                Iterables.getLast(result.getAttempts())); // real error
        throw new ConnectException("Kinesis Producer was not able to publish data - " + last.getErrorCode() + "-"
                + last.getErrorMessage());

    }

    private boolean validateOutStandingRecords() {
        if (pauseConsumption) {
            if (singleKinesisProducerPerPartition) {
                producerMap.values().forEach(producer -> {
                    int sleepCount = 0;
                    boolean pause = false;
                    // Validate if producer has outstanding records within
                    // threshold values
                    // and if not pause further consumption
                    while (producer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
                        try {
                            // Pausing further
                            sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray());
                            pause = true;
                            Thread.sleep(sleepPeriod);
                            if (sleepCount++ > sleepCycles) {
                                // Dummy message - Replace with your code to
                                // notify/log that Kinesis Producers have
                                // buffered values
                                // but are not being sent
                                log.error(
                                        "Kafka Consumption has been stopped because Kinesis Producers has buffered messages above threshold");
                                sleepCount = 0;
                            }
                        } catch (InterruptedException e) {
                            throw new ConnectException("Unable to publish records", e);
                        }
                    }
                    if (pause)
                        sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray());
                });
                return true;
            } else {
                int sleepCount = 0;
                boolean pause = false;
                // Validate if producer has outstanding records within threshold
                // values
                // and if not pause further consumption
                while (kinesisProducer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
                    try {
                        // Pausing further
                        sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray());
                        pause = true;
                        Thread.sleep(sleepPeriod);
                        if (sleepCount++ > sleepCycles) {
                            // Dummy message - Replace with your code to
                            // notify/log that Kinesis Producers have buffered
                            // values
                            // but are not being sent
                            log.error(
                                    "Kafka Consumption has been stopped because Kinesis Producers has buffered messages above threshold");
                            sleepCount = 0;
                        }
                    } catch (InterruptedException e) {
                        throw new ConnectException("Unable to publish records", e);
                    }
                }
                if (pause)
                    sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray());
                return true;
            }
        } else {
            return true;
        }
    }

    private ListenableFuture<UserRecordResult> addUserRecord(KinesisProducer kp, String streamName, String partitionKey,
                                                             boolean usePartitionAsHashKey, SinkRecord sinkRecord) {

        // If configured use kafka partition key as explicit hash key
        // This will be useful when sending data from same partition into
        // same shard
        if (usePartitionAsHashKey)
            return kp.addUserRecord(streamName, partitionKey, Integer.toString(sinkRecord.kafkaPartition()),
                    DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));
        else
            return kp.addUserRecord(streamName, partitionKey,
                    DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));

    }

    @Override
    public void start(Map<String, String> props) {

        streamName = props.get(AmazonKinesisSinkConnector.STREAM_NAME);

        maxConnections = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_CONNECTIONS));

        rateLimit = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RATE_LIMIT));

        maxBufferedTime = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_BUFFERED_TIME));

        ttl = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RECORD_TTL));

        regionName = props.get(AmazonKinesisSinkConnector.REGION);

        roleARN = props.get(AmazonKinesisSinkConnector.ROLE_ARN);

        baseRegionName = props.get(AmazonKinesisSinkConnector.BASE_REGION);

        baseRoleARN = props.get(AmazonKinesisSinkConnector.BASE_ROLE_ARN);

        roleSessionName = props.get(AmazonKinesisSinkConnector.ROLE_SESSION_NAME);

        roleDurationSeconds = Integer.parseInt(props.get(AmazonKinesisSinkConnector.ROLE_DURATION_SECONDS));

        roleExternalID = props.get(AmazonKinesisSinkConnector.ROLE_EXTERNAL_ID);

        kinesisEndpoint = props.get(AmazonKinesisSinkConnector.KINESIS_ENDPOINT);

        metricsLevel = props.get(AmazonKinesisSinkConnector.METRICS_LEVEL);

        metricsGranuality = props.get(AmazonKinesisSinkConnector.METRICS_GRANUALITY);

        metricsNameSpace = props.get(AmazonKinesisSinkConnector.METRICS_NAMESPACE);

        aggregration = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.AGGREGRATION_ENABLED));

        usePartitionAsHashKey = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.USE_PARTITION_AS_HASH_KEY));

        flushSync = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.FLUSH_SYNC));

        singleKinesisProducerPerPartition = Boolean
                .parseBoolean(props.get(AmazonKinesisSinkConnector.SINGLE_KINESIS_PRODUCER_PER_PARTITION));

        pauseConsumption = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.PAUSE_CONSUMPTION));

        outstandingRecordsThreshold = Integer
                .parseInt(props.get(AmazonKinesisSinkConnector.OUTSTANDING_RECORDS_THRESHOLD));

        sleepPeriod = Integer.parseInt(props.get(AmazonKinesisSinkConnector.SLEEP_PERIOD));

        sleepCycles = Integer.parseInt(props.get(AmazonKinesisSinkConnector.SLEEP_CYCLES));

        if (!singleKinesisProducerPerPartition)
            kinesisProducer = getKinesisProducer();

    }

    public void open(Collection<TopicPartition> partitions) {
        if (singleKinesisProducerPerPartition) {
            for (TopicPartition topicPartition : partitions) {
                producerMap.put(topicPartition.partition() + "@" + topicPartition.topic(), getKinesisProducer());
            }
        }
    }

    public void close(Collection<TopicPartition> partitions) {
        if (singleKinesisProducerPerPartition) {
            for (TopicPartition topicPartition : partitions) {
                producerMap.get(topicPartition.partition() + "@" + topicPartition.topic()).destroy();
                producerMap.remove(topicPartition.partition() + "@" + topicPartition.topic());
            }
        }
    }

    @Override
    public void stop() {
        // destroying kinesis producers which were not closed as part of close
        if (singleKinesisProducerPerPartition) {
            for (KinesisProducer kp : producerMap.values()) {
                kp.flushSync();
                kp.destroy();
            }
        } else {
            kinesisProducer.destroy();
        }

    }

    private KinesisProducer getKinesisProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(regionName);
        AWSCredentialsProvider baseProvider = IAMUtility.createCredentials(baseRegionName, baseRoleARN, null,
                RandomStringUtils.randomAlphanumeric(10), roleDurationSeconds, Optional.empty());
        config.setCredentialsProvider(IAMUtility.createCredentials(regionName, roleARN, roleExternalID, roleSessionName, roleDurationSeconds, Optional.of(baseProvider)));
        config.setMaxConnections(maxConnections);
        if (!StringUtils.isNullOrEmpty(kinesisEndpoint))
            config.setKinesisEndpoint(kinesisEndpoint);

        config.setAggregationEnabled(aggregration);

        // Limits the maximum allowed put rate for a shard, as a percentage of
        // the
        // backend limits.
        config.setRateLimit(rateLimit);

        // Maximum amount of time (milliseconds) a record may spend being
        // buffered
        // before it gets sent. Records may be sent sooner than this depending
        // on the
        // other buffering limits
        config.setRecordMaxBufferedTime(maxBufferedTime);

        // Set a time-to-live on records (milliseconds). Records that do not get
        // successfully put within the limit are failed.
        config.setRecordTtl(ttl);

        // Controls the number of metrics that are uploaded to CloudWatch.
        // Expected pattern: none|summary|detailed
        config.setMetricsLevel(metricsLevel);

        // Controls the granularity of metrics that are uploaded to CloudWatch.
        // Greater granularity produces more metrics.
        // Expected pattern: global|stream|shard
        config.setMetricsGranularity(metricsGranuality);

        // The namespace to upload metrics under.
        config.setMetricsNamespace(metricsNameSpace);

        return new KinesisProducer(config);

    }
}
