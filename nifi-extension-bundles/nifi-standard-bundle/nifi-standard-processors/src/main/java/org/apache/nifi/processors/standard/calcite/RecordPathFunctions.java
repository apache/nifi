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

package org.apache.nifi.processors.standard.calcite;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.sql.CalciteDatabase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordPathFunctions {

    public static void addToDatabase(final CalciteDatabase database) {
        database.addUserDefinedFunction("RPATH", ObjectRecordPath.class, "eval");
        database.addUserDefinedFunction("RPATH_STRING", StringRecordPath.class, "eval");
        database.addUserDefinedFunction("RPATH_INT", IntegerRecordPath.class, "eval");
        database.addUserDefinedFunction("RPATH_LONG", LongRecordPath.class, "eval");
        database.addUserDefinedFunction("RPATH_DATE", DateRecordPath.class, "eval");
        database.addUserDefinedFunction("RPATH_DOUBLE", DoubleRecordPath.class, "eval");
        database.addUserDefinedFunction("RPATH_FLOAT", FloatRecordPath.class, "eval");
    }

    public static class ObjectRecordPath extends RecordPathFunction {
        private static final RecordField ROOT_RECORD_FIELD = new RecordField("root", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
        private static final RecordSchema ROOT_RECORD_SCHEMA = new SimpleRecordSchema(List.of(ROOT_RECORD_FIELD));
        private static final RecordField PARENT_RECORD_FIELD = new RecordField("root", RecordFieldType.RECORD.getRecordDataType(ROOT_RECORD_SCHEMA));


        public Object eval(Object record, String recordPath) {
            if (record == null) {
                return null;
            }
            if (record instanceof Record) {
                return eval((Record) record, recordPath);
            }
            if (record instanceof Record[]) {
                return eval((Record[]) record, recordPath);
            }
            if (record instanceof Iterable) {
                return eval((Iterable<Record>) record, recordPath);
            }
            if (record instanceof Map) {
                return eval((Map<?, ?>) record, recordPath);
            }

            throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " against given argument because the argument is of type " + record.getClass() + " instead of Record");
        }

        private Object eval(final Map<?, ?> map, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final Record record = new MapRecord(ROOT_RECORD_SCHEMA, Collections.singletonMap("root", map));
            final FieldValue parentFieldValue = new StandardFieldValue(record, PARENT_RECORD_FIELD, null);
            final FieldValue fieldValue = new StandardFieldValue(map, ROOT_RECORD_FIELD, parentFieldValue);
            final RecordPathResult result = compiled.evaluate(record, fieldValue);

            final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());
            return evalResults(selectedFields);
        }

        private Object eval(final Record record, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);
            final RecordPathResult result = compiled.evaluate(record);

            final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());
            return evalResults(selectedFields);
        }

        private Object eval(final Iterable<Record> records, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final List<FieldValue> selectedFields = new ArrayList<>();
            for (final Record record : records) {
                final RecordPathResult result = compiled.evaluate(record);
                result.getSelectedFields().forEach(selectedFields::add);
            }

            return evalResults(selectedFields);
        }

        private Object eval(final Record[] records, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final List<FieldValue> selectedFields = new ArrayList<>();
            for (final Record record : records) {
                final RecordPathResult result = compiled.evaluate(record);
                result.getSelectedFields().forEach(selectedFields::add);
            }

            return evalResults(selectedFields);
        }

        private Object evalResults(final List<FieldValue> selectedFields) {
            if (selectedFields.isEmpty()) {
                return null;
            }

            if (selectedFields.size() == 1) {
                return selectedFields.getFirst().getValue();
            }

            return selectedFields.stream()
                .map(FieldValue::getValue)
                .toArray();
        }

    }

    public static class StringRecordPath extends RecordPathFunction {
        public String eval(Object record, String recordPath) {
            return eval(record, recordPath, Object::toString);
        }
    }

    public static class IntegerRecordPath extends RecordPathFunction {
        public Integer eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).intValue();
                }
                if (val instanceof String) {
                    return Integer.parseInt((String) val);
                }
                if (val instanceof Date) {
                    return (int) ((Date) val).getTime();
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Integer against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class LongRecordPath extends RecordPathFunction {
        public Long eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).longValue();
                }
                if (val instanceof String) {
                    return Long.parseLong((String) val);
                }
                if (val instanceof Date) {
                    return ((Date) val).getTime();
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Long against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class FloatRecordPath extends RecordPathFunction {
        public Float eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).floatValue();
                }
                if (val instanceof String) {
                    return Float.parseFloat((String) val);
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Float against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class DoubleRecordPath extends RecordPathFunction {
        public Double eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).doubleValue();
                }
                if (val instanceof String) {
                    return Double.parseDouble((String) val);
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Double against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class DateRecordPath extends RecordPathFunction {
        // Interestingly, Calcite throws an Exception if the schema indicates a DATE type and we return a java.util.Date. Calcite requires that a Long be returned instead.
        public Long eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).longValue();
                }
                if (val instanceof String) {
                    throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Date against " + record
                        + " because the value returned is of type String. To parse a String value as a Date, please use the toDate function. For example, " +
                        "SELECT RPATH_DATE( record, 'toDate( /event/timestamp, \"yyyy-MM-dd\" )' ) AS eventDate FROM FLOWFILE");
                }
                if (val instanceof Date) {
                    return ((Date) val).getTime();
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Date against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

}
