/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.amazon.kinesis.kafka;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Optional;

/**
 * IAMUtility offers convenience functions for creating AWS IAM credential providers.
 *
 */
public class IAMUtility {

    /**
     * Create an IAM credentials provider.
     *
     * If a role ARN is provided, then an STS assume-role credentials provider is created. The
     * provider will automatically renew the assume-role session as needed.
     *
     * If the role ARN is empty or null, then the default AWS credentials provider is returned.
     *
     * @param regionName AWS region-name (must be non-empty when using assume-role).
     * @param roleARN IAM role ARN to assume (if non-empty then STS assume-role provider is returned).
     * @param roleExternalID Optional external-id string to scope access within AWS account.
     * @param roleSessionName Optional role session-name used for logging & debugging.
     * @param roleDurationSeconds Duration of the STS assume-role session (auto-renewed on expiration).
     * @return AWS credentials provider
     */
    static AwsCredentialsProvider createCredentials(String regionName, String roleARN, String roleExternalID,
                                                    String roleSessionName, int roleDurationSeconds, Optional<AwsCredentialsProvider> baseProvider) {
        AwsCredentialsProvider previousProvider = baseProvider.orElse(DefaultCredentialsProvider.create());
        if (roleARN == null || roleARN.isEmpty())
            return previousProvider;

        // Use STS to assume a role if one was given
        final StsClient stsClient = StsClient.builder()
                .region(Region.of(regionName))
                .credentialsProvider(previousProvider)
                .build();

        AssumeRoleRequest.Builder requestBuilder = AssumeRoleRequest.builder()
                .roleArn(roleARN)
                .roleSessionName(roleSessionName);
        if (roleExternalID != null && !roleExternalID.isEmpty())
            requestBuilder = requestBuilder.externalId(roleExternalID);
        if (roleDurationSeconds > 0)
            requestBuilder = requestBuilder.durationSeconds(roleDurationSeconds);

        AssumeRoleRequest assumeRoleRequest = requestBuilder.build();

        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(assumeRoleRequest)
                .build();
    }
}
