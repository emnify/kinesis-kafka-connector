package com.amazon.kinesis.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.data.*;

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
