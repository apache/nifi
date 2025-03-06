/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.snowflake.snowpipe.streaming.converter;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter from arbitrary input objects to requested Snowflake Logical Data Type
 */
class ObjectLogicalDataTypeConverter {
    // Cutoff numbers defined according to Snowflake date and time input and output automatic detection
    private static final long MILLISECOND_CUTOFF = 31536000000L;
    private static final long MICROSECOND_CUTOFF = 31536000000000L;
    private static final long NANOSECOND_CUTOFF = 31536000000000000L;

    /**
     * Get object converted to Snowflake Logical Data Type
     *
     * @param object Input object to be converted
     * @param logicalDataType Snowflake Logical Data Type requested
     * @return Output object converted to Logical Data Type requested
     */
    public Object getConvertedObject(final Object object, final LogicalDataType logicalDataType) {
        if (object == null) {
            return null;
        }

        try {
            return switch (logicalDataType) {
                case UNKNOWN, BOOLEAN, TEXT, FIXED, REAL -> object;
                case ARRAY -> toObjectArray(object);
                case OBJECT -> toObjectMap(object);
                case DATE -> toLocalDate(object);
                case TIMESTAMP_NTZ, TIMESTAMP_LTZ, TIMESTAMP_TZ -> toInstant(object);
                case BINARY -> toByteArray(object);
                case TIME -> toLocalTime(object);
            };
        } catch (final Exception e) {
            throw new IllegalTypeConversionException("Conversion failed from [%s] to Snowflake Logical Type [%s]".formatted(object.getClass(), logicalDataType), e);
        }
    }

    private Map<String, Object> processNestedRecord(final Record record) {
        final Map<String, Object> output = new HashMap<>(record.toMap());
        for (final Map.Entry<String, Object> entry : output.entrySet()) {
            final Object value = entry.getValue();

            if (value instanceof Record valueMap) {
                entry.setValue(processNestedRecord(valueMap));
            } else if (value instanceof Object[] valueArray) {
                processNestedArray(valueArray);
            } else if (value instanceof Date date) {
                entry.setValue(toLocalDate(date));
            } else if (value instanceof Time time) {
                entry.setValue(toLocalTime(time));
            } else if (value instanceof Timestamp timestamp) {
                entry.setValue(toLocalDateTime(timestamp));
            }
        }
        return output;
    }

    private void processNestedArray(final Object[] valueArray) {
        for (int i = 0; i < valueArray.length; i++) {
            final Object value = valueArray[i];
            if (value instanceof Record record) {
                valueArray[i] = processNestedRecord(record);
            } else if (value instanceof Object[] array) {
                processNestedArray(array);
            } else if (value instanceof Date date) {
                valueArray[i] = toLocalDate(date);
            } else if (value instanceof Time time) {
                valueArray[i] = toLocalTime(time);
            } else if (value instanceof Timestamp timestamp) {
                valueArray[i] = toLocalDateTime(timestamp);
            }
        }
    }

    private Object toObjectMap(final Object object) {
        if (object instanceof Record record) {
            return processNestedRecord(record);
        }

        return object;
    }

    private Object toObjectArray(final Object rawValue) {
        if (rawValue instanceof Object[] array) {
            processNestedArray(array);
        }
        return rawValue;
    }

    private LocalDate toLocalDate(final Date date) {
        return date.toLocalDate();
    }

    private LocalTime toLocalTime(final Time time) {
        return time.toLocalTime();
    }

    private LocalDateTime toLocalDateTime(final Timestamp timestamp) {
        return timestamp.toLocalDateTime();
    }

    private byte[] toByteArray(final Object object) {
        if (object instanceof String string) {
            return Base64.getDecoder().decode(string);
        } else if (object instanceof Object[] objectArray) {
            return objectArrayToByteArray(objectArray);
        } else {
            throw new IllegalArgumentException("Failed to convert object [%s] to byte array".formatted(object));
        }
    }

    private byte[] objectArrayToByteArray(final Object[] values) {
        final byte[] byteValues = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            byteValues[i] = ((Number) values[i]).byteValue();
        }
        return byteValues;
    }

    private Object toLocalTime(final Object localTime) {
        return switch (localTime) {
            case final Time time:
                yield time.toLocalTime();
            case final Integer number:
                yield LocalTime.ofSecondOfDay(number);
            case final Long number:
                yield getInstantFromNumber(number).atZone(ZoneOffset.UTC).toLocalTime();
            default:
                yield localTime;
        };
    }

    private Object toLocalDate(final Object localDate) {
        return switch (localDate) {
            case final Date date:
                yield date.toLocalDate();
            case final Integer integer:
                yield Instant.ofEpochSecond(integer).atOffset(ZoneOffset.UTC).toLocalDate();
            case final Long longVal:
                yield getInstantFromNumber(longVal).atZone(ZoneOffset.UTC).toLocalDate();
            default:
                yield localDate;
        };
    }

    private Object toInstant(final Object object) {
        return switch (object) {
            case final Integer number:
                yield Instant.ofEpochSecond(number);
            case final Long number:
                yield getInstantFromNumber(number);
            case final Timestamp timestamp:
                yield timestamp.toInstant();
            default:
                yield object;
        };
    }

    private Instant getInstantFromNumber(final long number) {
        if (number < MILLISECOND_CUTOFF) {
            // Handle number as seconds
            return Instant.ofEpochSecond(number);
        } else if (number < MICROSECOND_CUTOFF) {
            // Handle number as milliseconds
            return Instant.ofEpochMilli(number);
        } else if (number < NANOSECOND_CUTOFF) {
            // Handle number as microseconds
            final long nanoseconds = number * 1000;
            return Instant.ofEpochSecond(0, nanoseconds);
        } else {
            // Handle number as nanoseconds
            return Instant.ofEpochSecond(0, number);
        }
    }
}
