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

/**
 * Utility methods for ORC support (conversion from Avro, conversion to Hive types, e.g.
 */
public class OrcUtils {

    public static void putToRowBatch(ColumnVector col, MutableInt vectorOffset, int rowNumber, Schema fieldSchema, Object o) {
        Schema.Type fieldType = fieldSchema.getType();

        if (fieldType == null) {
            throw new IllegalArgumentException("Field type is null");
        }

        if (Schema.Type.INT.equals(fieldType)) {
            if (o == null) {
                col.isNull[rowNumber] = true;
            } else {
                ((LongColumnVector) col).vector[rowNumber] = (int) o;
            }
        } else if (Schema.Type.LONG.equals(fieldType)) {
            if (o == null) {
                col.isNull[rowNumber] = true;
            } else {
                ((LongColumnVector) col).vector[rowNumber] = (long) o;
            }
        } else if (Schema.Type.BOOLEAN.equals(fieldType)) {
            if (o == null) {
                col.isNull[rowNumber] = true;
            } else {
                ((LongColumnVector) col).vector[rowNumber] = ((boolean) o) ? 1 : 0;
            }
        } else if (Schema.Type.BYTES.equals(fieldType)) {
            if (o == null) {
                col.isNull[rowNumber] = true;
            } else {
                ByteBuffer byteBuffer = ((ByteBuffer) o);
                int size = byteBuffer.remaining();
                byte[] buf = new byte[size];
                byteBuffer.get(buf, 0, size);
                ((BytesColumnVector) col).setVal(rowNumber, buf);
            }
        } else if (Schema.Type.DOUBLE.equals(fieldType)) {
            if (o == null) {
                col.isNull[rowNumber] = true;
            } else {
                ((DoubleColumnVector) col).vector[rowNumber] = (double) o;
            }
        } else if (Schema.Type.FLOAT.equals(fieldType)) {
            if (o == null) {
                col.isNull[rowNumber] = true;
            } else {
                ((DoubleColumnVector) col).vector[rowNumber] = (float) o;
            }
        } else if (Schema.Type.STRING.equals(fieldType) || Schema.Type.ENUM.equals(fieldType)) {
            if (o == null) {
                col.isNull[rowNumber] = true;
            } else {
                ((BytesColumnVector) col).setVal(rowNumber, o.toString().getBytes());
            }
        } else if (Schema.Type.UNION.equals(fieldType)) {
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

        } else if (Schema.Type.ARRAY.equals(fieldType)) {
            Schema arrayType = fieldSchema.getElementType();
            ListColumnVector array = ((ListColumnVector) col);
            if (o instanceof int[]) {
                int[] intArray = (int[]) o;
                for (int i = 0; i < intArray.length; i++) {
                    ((LongColumnVector) array.child).vector[vectorOffset.getValue() + i] = intArray[i];
                }
                array.offsets[rowNumber] = vectorOffset.longValue();
                array.lengths[rowNumber] = intArray.length;
                vectorOffset.add(intArray.length);
            } else if (o instanceof long[]) {
                long[] longArray = (long[]) o;
                for (int i = 0; i < longArray.length; i++) {
                    ((LongColumnVector) array.child).vector[vectorOffset.getValue() + i] = longArray[i];
                }
                array.offsets[rowNumber] = vectorOffset.longValue();
                array.lengths[rowNumber] = longArray.length;
                vectorOffset.add(longArray.length);
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

        } else if (Schema.Type.MAP.equals(fieldType)) {
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

        } else {
            throw new IllegalArgumentException("Field type " + fieldType.getName() + " not recognized");
        }

    }

    public static String normalizeHiveTableName(String name) {
        return name.replaceAll("[\\. ]", "_");
    }

    public static String generateHiveDDL(Schema avroSchema, String tableName) {
        Schema.Type schemaType = avroSchema.getType();
        StringBuffer sb = new StringBuffer("CREATE EXTERNAL TABLE IF NOT EXISTS ");
        sb.append(tableName);
        sb.append(" (");
        if (Schema.Type.RECORD.equals(schemaType)) {
            List<String> hiveColumns = new ArrayList<>();
            List<Schema.Field> fields = avroSchema.getFields();
            if (fields != null) {
                for (Schema.Field field : fields) {
                    hiveColumns.add(field.name() + " " + getHiveTypeFromAvroType(field.schema()));
                }
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

        if (Schema.Type.INT.equals(fieldType)
                || Schema.Type.LONG.equals(fieldType)
                || Schema.Type.BOOLEAN.equals(fieldType)
                || Schema.Type.BYTES.equals(fieldType)
                || Schema.Type.DOUBLE.equals(fieldType)
                || Schema.Type.FLOAT.equals(fieldType)
                || Schema.Type.STRING.equals(fieldType)) {

            return getPrimitiveOrcTypeFromPrimitiveAvroType(fieldType);

        } else if (Schema.Type.UNION.equals(fieldType)) {
            List<Schema> unionFieldSchemas = fieldSchema.getTypes();
            TypeDescription unionSchema = TypeDescription.createUnion();
            if (unionFieldSchemas != null) {
                List<TypeDescription> orcFields = new ArrayList<>();
                for (Schema unionFieldSchema : unionFieldSchemas) {

                    // Ignore null types in union
                    if (!Schema.Type.NULL.equals(unionFieldSchema.getType())) {
                        orcFields.add(getOrcField(unionFieldSchema));
                    }
                }
                // Flatten the field if the union only has one non-null element
                if (orcFields.size() == 1) {
                    return orcFields.get(0);
                } else {
                    for (TypeDescription unionType : orcFields) {
                        unionSchema.addUnionChild(unionType);
                    }
                }
            }

            return unionSchema;

        } else if (Schema.Type.ARRAY.equals(fieldType)) {
            return TypeDescription.createList(getOrcField(fieldSchema.getElementType()));
        } else if (Schema.Type.MAP.equals(fieldType)) {

            return TypeDescription.createMap(
                    TypeDescription.createString(),
                    getOrcField(fieldSchema.getValueType()));


        } else if (Schema.Type.RECORD.equals(fieldType)) {

            TypeDescription record = TypeDescription.createStruct();
            List<Schema.Field> avroFields = fieldSchema.getFields();
            if (avroFields != null) {
                for (Schema.Field avroField : avroFields) {
                    addOrcField(record, avroField);
                }
            }
            return record;

        } else if (Schema.Type.ENUM.equals(fieldType)) {
            // An enum value is just a String for ORC/Hive
            return TypeDescription.createString();

        } else {
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
        } else if (Schema.Type.INT.equals(avroType)) {
            return TypeDescription.createInt();
        } else if (Schema.Type.LONG.equals(avroType)) {
            return TypeDescription.createLong();
        } else if (Schema.Type.BOOLEAN.equals(avroType)) {
            return TypeDescription.createBoolean();
        } else if (Schema.Type.BYTES.equals(avroType)) {
            return TypeDescription.createBinary();
        } else if (Schema.Type.DOUBLE.equals(avroType)) {
            return TypeDescription.createDouble();
        } else if (Schema.Type.FLOAT.equals(avroType)) {
            return TypeDescription.createFloat();
        } else if (Schema.Type.STRING.equals(avroType)) {
            return TypeDescription.createString();
        } else {
            throw new IllegalArgumentException("Avro type " + avroType.getName() + " is not a primitive type");
        }
    }

    public static String getHiveTypeFromAvroType(Schema avroSchema) {
        if (avroSchema == null) {
            throw new IllegalArgumentException("Avro schema is null");
        }

        Schema.Type avroType = avroSchema.getType();

        if (Schema.Type.INT.equals(avroType)) {
            return "INT";
        } else if (Schema.Type.LONG.equals(avroType)) {
            return "BIGINT";
        } else if (Schema.Type.BOOLEAN.equals(avroType)) {
            return "BOOLEAN";
        } else if (Schema.Type.BYTES.equals(avroType)) {
            return "BINARY";
        } else if (Schema.Type.DOUBLE.equals(avroType)) {
            return "DOUBLE";
        } else if (Schema.Type.FLOAT.equals(avroType)) {
            return "FLOAT";
        } else if (Schema.Type.STRING.equals(avroType) || Schema.Type.ENUM.equals(avroType)) {
            return "STRING";
        } else if (Schema.Type.UNION.equals(avroType)) {
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
                if (hiveFields.size() == 1) {
                    return hiveFields.get(0);
                } else {
                    StringBuilder sb = new StringBuilder("UNIONTYPE<");
                    sb.append(StringUtils.join(hiveFields, ", "));
                    sb.append(">");
                    return sb.toString();
                }
            }

        } else if (Schema.Type.MAP.equals(avroType)) {
            return "MAP<STRING, " + getHiveTypeFromAvroType(avroSchema.getValueType()) + ">";
        } else if (Schema.Type.ARRAY.equals(avroType)) {
            return "ARRAY<" + getHiveTypeFromAvroType(avroSchema.getElementType()) + ">";
        } else if (Schema.Type.RECORD.equals(avroType)) {
            List<Schema.Field> recordFields = avroSchema.getFields();
            if (recordFields != null) {
                List<String> hiveFields = new ArrayList<>();
                for (Schema.Field recordField : recordFields) {
                    hiveFields.add(recordField.name() + ":" + getHiveTypeFromAvroType(recordField.schema()));
                }
                StringBuilder sb = new StringBuilder("STRUCT<");
                sb.append(StringUtils.join(hiveFields, ", "));
                sb.append(">");
                return sb.toString();
            }
        }

        throw new IllegalArgumentException("Error converting Avro type " + avroType.getName() + " to Hive type");
    }
}
