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
package org.apache.nifi.util.orc;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility methods for ORC support (conversion from Avro, conversion to Hive types, e.g.
 */
public class OrcUtils {

    public static void putToRowBatch(ColumnVector col, MutableInt vectorOffset, int rowNumber, Schema fieldSchema, Object o) {
        Schema.Type fieldType = fieldSchema.getType();

        if (fieldType == null) {
            throw new IllegalArgumentException("Field type is null");
        }

        if (o == null) {
            col.isNull[rowNumber] = true;
        } else {

            switch (fieldType) {
                case INT:
                    ((LongColumnVector) col).vector[rowNumber] = (int) o;
                    break;
                case LONG:
                    ((LongColumnVector) col).vector[rowNumber] = (long) o;
                    break;
                case BOOLEAN:
                    ((LongColumnVector) col).vector[rowNumber] = ((boolean) o) ? 1 : 0;
                    break;
                case BYTES:
                    ByteBuffer byteBuffer = ((ByteBuffer) o);
                    int size = byteBuffer.remaining();
                    byte[] buf = new byte[size];
                    byteBuffer.get(buf, 0, size);
                    ((BytesColumnVector) col).setVal(rowNumber, buf);
                    break;
                case DOUBLE:
                    ((DoubleColumnVector) col).vector[rowNumber] = (double) o;
                    break;
                case FLOAT:
                    ((DoubleColumnVector) col).vector[rowNumber] = (float) o;
                    break;
                case STRING:
                case ENUM:
                    ((BytesColumnVector) col).setVal(rowNumber, o.toString().getBytes());
                    break;
                case UNION:
                    // If the union only has one non-null type in it, it was flattened in the ORC schema
                    if (col instanceof UnionColumnVector) {
                        UnionColumnVector union = ((UnionColumnVector) col);
                        Schema.Type avroType = OrcUtils.getAvroSchemaTypeOfObject(o);
                        // Find the index in the union with the matching Avro type
                        int unionIndex = -1;
                        List<Schema> types = fieldSchema.getTypes();
                        final int numFields = types.size();
                        for (int i = 0; i < numFields && unionIndex == -1; i++) {
                            if (avroType.equals(types.get(i).getType())) {
                                unionIndex = i;
                            }
                        }
                        if (unionIndex == -1) {
                            throw new IllegalArgumentException("Object type " + avroType.getName() + " not found in union '" + fieldSchema.getName() + "'");
                        }

                        // Need nested vector offsets
                        MutableInt unionVectorOffset = new MutableInt(0);
                        putToRowBatch(union.fields[unionIndex], unionVectorOffset, rowNumber, fieldSchema.getTypes().get(unionIndex), o);
                    } else {
                        // Find and use the non-null type from the union
                        List<Schema> types = fieldSchema.getTypes();
                        Schema effectiveType = null;
                        for (Schema type : types) {
                            if (!Schema.Type.NULL.equals(type.getType())) {
                                effectiveType = type;
                                break;
                            }
                        }
                        putToRowBatch(col, vectorOffset, rowNumber, effectiveType, o);
                    }
                    break;
                case ARRAY:
                    Schema arrayType = fieldSchema.getElementType();
                    ListColumnVector array = ((ListColumnVector) col);
                    if (o instanceof int[] || o instanceof long[]) {
                        int length = (o instanceof int[]) ? ((int[]) o).length : ((long[]) o).length;
                        for (int i = 0; i < length; i++) {
                            ((LongColumnVector) array.child).vector[vectorOffset.getValue() + i] =
                                    (o instanceof int[]) ? ((int[]) o)[i] : ((long[]) o)[i];
                        }
                        array.offsets[rowNumber] = vectorOffset.longValue();
                        array.lengths[rowNumber] = length;
                        vectorOffset.add(length);
                    } else if (o instanceof float[]) {
                        float[] floatArray = (float[]) o;
                        for (int i = 0; i < floatArray.length; i++) {
                            ((DoubleColumnVector) array.child).vector[vectorOffset.getValue() + i] = floatArray[i];
                        }
                        array.offsets[rowNumber] = vectorOffset.longValue();
                        array.lengths[rowNumber] = floatArray.length;
                        vectorOffset.add(floatArray.length);
                    } else if (o instanceof double[]) {
                        double[] doubleArray = (double[]) o;
                        for (int i = 0; i < doubleArray.length; i++) {
                            ((DoubleColumnVector) array.child).vector[vectorOffset.getValue() + i] = doubleArray[i];
                        }
                        array.offsets[rowNumber] = vectorOffset.longValue();
                        array.lengths[rowNumber] = doubleArray.length;
                        vectorOffset.add(doubleArray.length);
                    } else if (o instanceof String[]) {
                        String[] stringArray = (String[]) o;
                        BytesColumnVector byteCol = ((BytesColumnVector) array.child);
                        for (int i = 0; i < stringArray.length; i++) {
                            if (stringArray[i] == null) {
                                byteCol.isNull[rowNumber] = true;
                            } else {
                                byteCol.setVal(vectorOffset.getValue() + i, stringArray[i].getBytes());
                            }
                        }
                        array.offsets[rowNumber] = vectorOffset.longValue();
                        array.lengths[rowNumber] = stringArray.length;
                        vectorOffset.add(stringArray.length);
                    } else if (o instanceof Map[]) {
                        Map[] mapArray = (Map[]) o;
                        MutableInt mapVectorOffset = new MutableInt(0);
                        for (int i = 0; i < mapArray.length; i++) {
                            if (mapArray[i] == null) {
                                array.child.isNull[rowNumber] = true;
                            } else {
                                putToRowBatch(array.child, mapVectorOffset, vectorOffset.getValue() + i, arrayType, mapArray[i]);
                            }
                        }
                        array.offsets[rowNumber] = vectorOffset.longValue();
                        array.lengths[rowNumber] = mapArray.length;
                        vectorOffset.add(mapArray.length);
                    } else if (o instanceof List) {
                        List listArray = (List) o;
                        MutableInt listVectorOffset = new MutableInt(0);
                        int numElements = listArray.size();
                        for (int i = 0; i < numElements; i++) {
                            if (listArray.get(i) == null) {
                                array.child.isNull[rowNumber] = true;
                            } else {
                                putToRowBatch(array.child, listVectorOffset, vectorOffset.getValue() + i, arrayType, listArray.get(i));
                            }
                        }
                        array.offsets[rowNumber] = vectorOffset.longValue();
                        array.lengths[rowNumber] = numElements;
                        vectorOffset.add(numElements);

                    } else {
                        throw new IllegalArgumentException("Object class " + o.getClass().getName() + " not supported as an ORC list/array");
                    }
                    break;
                case MAP:
                    MapColumnVector map = ((MapColumnVector) col);

                    // Avro maps require String keys
                    @SuppressWarnings("unchecked")
                    Map<String, ?> mapObj = (Map<String, ?>) o;
                    int effectiveRowNumber = vectorOffset.getValue();
                    for (Map.Entry<String, ?> entry : mapObj.entrySet()) {
                        putToRowBatch(map.keys, vectorOffset, effectiveRowNumber, Schema.create(Schema.Type.STRING), entry.getKey());
                        putToRowBatch(map.values, vectorOffset, effectiveRowNumber, fieldSchema.getValueType(), entry.getValue());
                        effectiveRowNumber++;
                    }
                    map.offsets[rowNumber] = vectorOffset.longValue();
                    map.lengths[rowNumber] = mapObj.size();
                    vectorOffset.add(mapObj.size());

                    break;
                default:
                    throw new IllegalArgumentException("Field type " + fieldType.getName() + " not recognized");
            }
        }
    }

    public static String normalizeHiveTableName(String name) {
        return name.replaceAll("[\\. ]", "_");
    }

    public static String generateHiveDDL(Schema avroSchema, String tableName) {
        Schema.Type schemaType = avroSchema.getType();
        StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE IF NOT EXISTS ");
        sb.append(tableName);
        sb.append(" (");
        if (Schema.Type.RECORD.equals(schemaType)) {
            List<String> hiveColumns = new ArrayList<>();
            List<Schema.Field> fields = avroSchema.getFields();
            if (fields != null) {
                hiveColumns.addAll(
                        fields.stream().map(field -> field.name() + " " + getHiveTypeFromAvroType(field.schema())).collect(Collectors.toList()));
            }
            sb.append(StringUtils.join(hiveColumns, ", "));
            sb.append(") STORED AS ORC");
            return sb.toString();
        } else {
            throw new IllegalArgumentException("Avro schema is of type " + schemaType.getName() + ", not RECORD");
        }
    }


    public static void addOrcField(TypeDescription orcSchema, Schema.Field avroField) {
        Schema fieldSchema = avroField.schema();
        String fieldName = avroField.name();

        orcSchema.addField(fieldName, getOrcField(fieldSchema));
    }

    public static TypeDescription getOrcField(Schema fieldSchema) throws IllegalArgumentException {
        Schema.Type fieldType = fieldSchema.getType();

        switch (fieldType) {
            case INT:
            case LONG:
            case BOOLEAN:
            case BYTES:
            case DOUBLE:
            case FLOAT:
            case STRING:
                return getPrimitiveOrcTypeFromPrimitiveAvroType(fieldType);

            case UNION:
                List<Schema> unionFieldSchemas = fieldSchema.getTypes();
                TypeDescription unionSchema = TypeDescription.createUnion();
                if (unionFieldSchemas != null) {
                    // Ignore null types in union
                    List<TypeDescription> orcFields = unionFieldSchemas.stream().filter(
                            unionFieldSchema -> !Schema.Type.NULL.equals(unionFieldSchema.getType())).map(OrcUtils::getOrcField).collect(Collectors.toList());


                    // Flatten the field if the union only has one non-null element
                    if (orcFields.size() == 1) {
                        return orcFields.get(0);
                    } else {
                        orcFields.forEach(unionSchema::addUnionChild);
                    }
                }
                return unionSchema;

            case ARRAY:
                return TypeDescription.createList(getOrcField(fieldSchema.getElementType()));

            case MAP:
                return TypeDescription.createMap(TypeDescription.createString(), getOrcField(fieldSchema.getValueType()));

            case RECORD:
                TypeDescription record = TypeDescription.createStruct();
                List<Schema.Field> avroFields = fieldSchema.getFields();
                if (avroFields != null) {
                    avroFields.forEach(avroField -> addOrcField(record, avroField));
                }
                return record;

            case ENUM:
                // An enum value is just a String for ORC/Hive
                return TypeDescription.createString();

            default:
                throw new IllegalArgumentException("Did not recognize Avro type " + fieldType.getName());
        }

    }

    public static Schema.Type getAvroSchemaTypeOfObject(Object o) {
        if (o == null) {
            return Schema.Type.NULL;
        } else if (o instanceof Integer) {
            return Schema.Type.INT;
        } else if (o instanceof Long) {
            return Schema.Type.LONG;
        } else if (o instanceof Boolean) {
            return Schema.Type.BOOLEAN;
        } else if (o instanceof byte[]) {
            return Schema.Type.BYTES;
        } else if (o instanceof Float) {
            return Schema.Type.FLOAT;
        } else if (o instanceof Double) {
            return Schema.Type.DOUBLE;
        } else if (o instanceof Enum) {
            return Schema.Type.ENUM;
        } else if (o instanceof Object[]) {
            return Schema.Type.ARRAY;
        } else if (o instanceof List) {
            return Schema.Type.ARRAY;
        } else if (o instanceof Map) {
            return Schema.Type.MAP;
        } else {
            throw new IllegalArgumentException("Object of class " + o.getClass() + " is not a supported Avro Type");
        }
    }

    public static TypeDescription getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type avroType) throws IllegalArgumentException {
        if (avroType == null) {
            throw new IllegalArgumentException("Avro type is null");
        }
        switch (avroType) {
            case INT:
                return TypeDescription.createInt();
            case LONG:
                return TypeDescription.createLong();
            case BOOLEAN:
                return TypeDescription.createBoolean();
            case BYTES:
                return TypeDescription.createBinary();
            case DOUBLE:
                return TypeDescription.createDouble();
            case FLOAT:
                return TypeDescription.createFloat();
            case STRING:
                return TypeDescription.createString();
            default:
                throw new IllegalArgumentException("Avro type " + avroType.getName() + " is not a primitive type");
        }
    }

    public static String getHiveTypeFromAvroType(Schema avroSchema) {
        if (avroSchema == null) {
            throw new IllegalArgumentException("Avro schema is null");
        }

        Schema.Type avroType = avroSchema.getType();

        switch (avroType) {
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case BOOLEAN:
                return "BOOLEAN";
            case BYTES:
                return "BINARY";
            case DOUBLE:
                return "DOUBLE";
            case FLOAT:
                return "FLOAT";
            case STRING:
            case ENUM:
                return "STRING";
            case UNION:
                List<Schema> unionFieldSchemas = avroSchema.getTypes();
                if (unionFieldSchemas != null) {
                    List<String> hiveFields = new ArrayList<>();
                    for (Schema unionFieldSchema : unionFieldSchemas) {
                        Schema.Type unionFieldSchemaType = unionFieldSchema.getType();
                        // Ignore null types in union
                        if (!Schema.Type.NULL.equals(unionFieldSchemaType)) {
                            hiveFields.add(getHiveTypeFromAvroType(unionFieldSchema));
                        }
                    }
                    // Flatten the field if the union only has one non-null element
                    return (hiveFields.size() == 1)
                            ? hiveFields.get(0)
                            : "UNIONTYPE<" + StringUtils.join(hiveFields, ", ") + ">";

                }
                break;
            case MAP:
                return "MAP<STRING, " + getHiveTypeFromAvroType(avroSchema.getValueType()) + ">";
            case ARRAY:
                return "ARRAY<" + getHiveTypeFromAvroType(avroSchema.getElementType()) + ">";
            case RECORD:
                List<Schema.Field> recordFields = avroSchema.getFields();
                if (recordFields != null) {
                    List<String> hiveFields = recordFields.stream().map(
                            recordField -> recordField.name() + ":" + getHiveTypeFromAvroType(recordField.schema())).collect(Collectors.toList());
                    return "STRUCT<" + StringUtils.join(hiveFields, ", ") + ">";
                }
                break;
            default:
                break;
        }

        throw new IllegalArgumentException("Error converting Avro type " + avroType.getName() + " to Hive type");
    }
}
