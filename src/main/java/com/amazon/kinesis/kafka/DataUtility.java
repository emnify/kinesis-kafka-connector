package com.amazon.kinesis.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.nio.Buffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;

public class DataUtility {

	/**
	 * Parses Kafka Values
	 * 
	 * @param schema
	 *            - Schema of passed message as per
	 *            https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.html
	 * @param value
	 *            - Value of the message
	 * @return Parsed bytebuffer as per schema type
	 */
	public static ByteBuffer parseValue(Schema schema, Object rawValue) {
		Schema.Type t = schema.type();
		Object value = toPrimitiveValue(schema, rawValue);
		switch (t) {
		case INT8:
			ByteBuffer byteBuffer = ByteBuffer.allocate(1);
			byteBuffer.put((Byte) value);
			return byteBuffer;
		case INT16:
			ByteBuffer shortBuf = ByteBuffer.allocate(2);
			shortBuf.putShort((Short) value);
			return shortBuf;
		case INT32:
			ByteBuffer intBuf = ByteBuffer.allocate(4);
			intBuf.putInt((Integer) value);
			return intBuf;
		case INT64:
			ByteBuffer longBuf = ByteBuffer.allocate(8);
			longBuf.putLong((Long) value);
			return longBuf;
		case FLOAT32:
			ByteBuffer floatBuf = ByteBuffer.allocate(4);
			floatBuf.putFloat((Float) value);
			return floatBuf;
		case FLOAT64:
			ByteBuffer doubleBuf = ByteBuffer.allocate(8);
			doubleBuf.putDouble((Double) value);
			return doubleBuf;
		case BOOLEAN:
			ByteBuffer boolBuffer = ByteBuffer.allocate(1);
			boolBuffer.put((byte) ((Boolean) value ? 1 : 0));
			return boolBuffer;
		case STRING:
            return ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8));
		case BYTES:
			if (value instanceof byte[]) {
				return ByteBuffer.wrap((byte[]) value);
			} else if (value instanceof ByteBuffer) {
				return (ByteBuffer) value;
			}
		case ARRAY:
			Schema sch = schema.valueSchema();
			if (sch.type() == Schema.Type.MAP || sch.type() == Schema.Type.STRUCT) {
				throw new DataException("Invalid schema type.");
			}
			Object[] objs = (Object[]) value;
			ByteBuffer[] byteBuffers = new ByteBuffer[objs.length];
			int noOfByteBuffer = 0;

			for (Object obj : objs) {
				byteBuffers[noOfByteBuffer++] = parseValue(sch, obj);
			}

			ByteBuffer result = ByteBuffer.allocate(Arrays.stream(byteBuffers).mapToInt(Buffer::remaining).sum());
			Arrays.stream(byteBuffers).forEach(bb -> result.put(bb.duplicate()));
			return result;
  	    case STRUCT:
		   List<ByteBuffer> fieldList = new LinkedList<ByteBuffer>();
		   // Parsing each field of structure
		   schema.fields().forEach(field -> fieldList.add(parseValue(field.schema(), ((Struct) value).get(field))));
		   // Initialize ByteBuffer
		   ByteBuffer processedValue = ByteBuffer.allocate(fieldList.stream().mapToInt(Buffer::remaining).sum());
		   // Combine bytebuffer of all fields
		   fieldList.forEach(buffer -> processedValue.put(buffer.duplicate()));

		   return processedValue;
		}
		return null;
	}

	private static Object toPrimitiveValue(Schema schema, Object value) {
    	String schemaName = schema.name();
    	Object defaultValue = schema.defaultValue();
    	if (schemaName == null) {
    		return value;
		}
    	switch (schemaName) {
			case Date.LOGICAL_NAME:
			case Time.LOGICAL_NAME:
			case Timestamp.LOGICAL_NAME:
				java.util.Date dateValue = (java.util.Date)(value == null ? defaultValue : value);
				return dateValue.getTime();
			case Decimal.LOGICAL_NAME:
			    return (double)value;
			default:
				// return value by default. Schema name can have various names
				return value;
		}
	}

}
