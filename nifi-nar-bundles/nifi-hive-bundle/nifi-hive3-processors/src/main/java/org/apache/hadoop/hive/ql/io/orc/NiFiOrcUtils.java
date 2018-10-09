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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcConf;
import org.apache.orc.impl.MemoryManagerImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Utility methods for ORC support (conversion from Avro, conversion to Hive types, e.g.
 */
public class NiFiOrcUtils {

    public static Object convertToORCObject(TypeInfo typeInfo, Object o, final boolean hiveFieldNames) {
        if (o != null) {
            if (typeInfo instanceof UnionTypeInfo) {
                OrcUnion union = new OrcUnion();
                // Need to find which of the union types correspond to the primitive object
                TypeInfo objectTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(
                        ObjectInspectorFactory.getReflectionObjectInspector(o.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
                List<TypeInfo> unionTypeInfos = ((UnionTypeInfo) typeInfo).getAllUnionObjectTypeInfos();

                int index = 0;
                while (index < unionTypeInfos.size() && !unionTypeInfos.get(index).equals(objectTypeInfo)) {
                    index++;
                }
                if (index < unionTypeInfos.size()) {
                    union.set((byte) index, convertToORCObject(objectTypeInfo, o, hiveFieldNames));
                } else {
                    throw new IllegalArgumentException("Object Type for class " + o.getClass().getName() + " not in Union declaration");
                }
                return union;
            }
            if (o instanceof Integer) {
                return new IntWritable((int) o);
            }
            if (o instanceof Boolean) {
                return new BooleanWritable((boolean) o);
            }
            if (o instanceof Long) {
                return new LongWritable((long) o);
            }
            if (o instanceof Float) {
                return new FloatWritable((float) o);
            }
            if (o instanceof Double) {
                return new DoubleWritable((double) o);
            }
            if (o instanceof String) {
                return new Text(o.toString());
            }
            if (o instanceof ByteBuffer) {
                return new BytesWritable(((ByteBuffer) o).array());
            }
            if (o instanceof Timestamp) {
                Timestamp t = (Timestamp) o;
                org.apache.hadoop.hive.common.type.Timestamp timestamp = new org.apache.hadoop.hive.common.type.Timestamp();
                timestamp.setTimeInMillis(t.getTime(), t.getNanos());
                return new TimestampWritableV2(timestamp);
            }
            if (o instanceof Date) {
                Date d = (Date) o;
                org.apache.hadoop.hive.common.type.Date date = new org.apache.hadoop.hive.common.type.Date();
                date.setTimeInMillis(d.getTime());
                return new DateWritableV2(date);
            }
            if (o instanceof Object[]) {
                Object[] objArray = (Object[]) o;
                if(TypeInfoFactory.binaryTypeInfo.equals(typeInfo)) {
                    byte[] dest = new byte[objArray.length];
                    for(int i=0;i<objArray.length;i++) {
                        dest[i] = (byte) objArray[i];
                    }
                    return new BytesWritable(dest);
                } else {
                    // If not binary, assume a list of objects
                    TypeInfo listTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
                    return Arrays.stream(objArray)
                            .map(o1 -> convertToORCObject(listTypeInfo, o1, hiveFieldNames))
                            .collect(Collectors.toList());
                }
            }
            if (o instanceof int[]) {
                int[] intArray = (int[]) o;
                return Arrays.stream(intArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("int"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof long[]) {
                long[] longArray = (long[]) o;
                return Arrays.stream(longArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("bigint"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof float[]) {
                float[] floatArray = (float[]) o;
                return IntStream.range(0, floatArray.length)
                        .mapToDouble(i -> floatArray[i])
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("float"), (float) element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof double[]) {
                double[] doubleArray = (double[]) o;
                return Arrays.stream(doubleArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("double"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof boolean[]) {
                boolean[] booleanArray = (boolean[]) o;
                return IntStream.range(0, booleanArray.length)
                        .map(i -> booleanArray[i] ? 1 : 0)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("boolean"), element == 1, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof List) {
                return o;
            }
            if (o instanceof Record) {
                Record record = (Record) o;
                TypeInfo recordSchema = NiFiOrcUtils.getOrcSchema(record.getSchema(), hiveFieldNames);
                List<RecordField> recordFields = record.getSchema().getFields();
                if (recordFields != null) {
                    Object[] fieldObjects = new Object[recordFields.size()];
                    for (int i = 0; i < recordFields.size(); i++) {
                        RecordField field = recordFields.get(i);
                        DataType dataType = field.getDataType();
                        Object fieldObject = record.getValue(field);
                        fieldObjects[i] = convertToORCObject(NiFiOrcUtils.getOrcField(dataType, hiveFieldNames), fieldObject, hiveFieldNames);
                    }
                    return NiFiOrcUtils.createOrcStruct(recordSchema, fieldObjects);
                }
                return null;
            }
            if (o instanceof Map) {
                Map map = new HashMap();
                MapTypeInfo mapTypeInfo = ((MapTypeInfo) typeInfo);
                TypeInfo keyInfo = mapTypeInfo.getMapKeyTypeInfo();
                TypeInfo valueInfo = mapTypeInfo.getMapValueTypeInfo();
                // Unions are not allowed as key/value types, so if we convert the key and value objects,
                // they should return Writable objects
                ((Map) o).forEach((key, value) -> {
                    Object keyObject = convertToORCObject(keyInfo, key, hiveFieldNames);
                    Object valueObject = convertToORCObject(valueInfo, value, hiveFieldNames);
                    if (keyObject == null) {
                        throw new IllegalArgumentException("Maps' key cannot be null");
                    }
                    map.put(keyObject, valueObject);
                });
                return map;
            }
            throw new IllegalArgumentException("Error converting object of type " + o.getClass().getName() + " to ORC type " + typeInfo.getTypeName());
        } else {
            return null;
        }
    }


    /**
     * Create an object of OrcStruct given a TypeInfo and a list of objects
     *
     * @param typeInfo The TypeInfo object representing the ORC record schema
     * @param objs     ORC objects/Writables
     * @return an OrcStruct containing the specified objects for the specified schema
     */
    @SuppressWarnings("unchecked")
    public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object... objs) {
        SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct
                .createObjectInspector(typeInfo);
        List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
        OrcStruct result = (OrcStruct) oi.create();
        result.setNumFields(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            oi.setStructFieldData(result, fields.get(i), objs[i]);
        }
        return result;
    }

    public static String normalizeHiveTableName(String name) {
        return name.replaceAll("[\\. ]", "_");
    }

    public static String generateHiveDDL(RecordSchema recordSchema, String tableName, boolean hiveFieldNames) {
        StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE IF NOT EXISTS `");
        sb.append(tableName);
        sb.append("` (");
        List<String> hiveColumns = new ArrayList<>();
        List<RecordField> fields = recordSchema.getFields();
        if (fields != null) {
            hiveColumns.addAll(
                    fields.stream().map(field -> "`" + (hiveFieldNames ? field.getFieldName().toLowerCase() : field.getFieldName()) + "` "
                            + getHiveTypeFromFieldType(field.getDataType(), hiveFieldNames)).collect(Collectors.toList()));
        }
        sb.append(StringUtils.join(hiveColumns, ", "));
        sb.append(") STORED AS ORC");
        return sb.toString();

    }

    public static TypeInfo getOrcSchema(RecordSchema recordSchema, boolean hiveFieldNames) throws IllegalArgumentException {
        List<RecordField> recordFields = recordSchema.getFields();
        if (recordFields != null) {
            List<String> orcFieldNames = new ArrayList<>(recordFields.size());
            List<TypeInfo> orcFields = new ArrayList<>(recordFields.size());
            recordFields.forEach(recordField -> {
                String fieldName = hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName();
                orcFieldNames.add(fieldName);
                orcFields.add(getOrcField(recordField.getDataType(), hiveFieldNames));
            });
            return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
        }
        return null;
    }


    public static TypeInfo getOrcField(DataType dataType, boolean hiveFieldNames) throws IllegalArgumentException {
        if (dataType == null) {
            return null;
        }

        RecordFieldType fieldType = dataType.getFieldType();
        if (RecordFieldType.INT.equals(fieldType)
                || RecordFieldType.LONG.equals(fieldType)
                || RecordFieldType.BOOLEAN.equals(fieldType)
                || RecordFieldType.DOUBLE.equals(fieldType)
                || RecordFieldType.FLOAT.equals(fieldType)
                || RecordFieldType.STRING.equals(fieldType)) {
            return getPrimitiveOrcTypeFromPrimitiveFieldType(dataType);
        }
        if (RecordFieldType.DATE.equals(fieldType)) {
            return TypeInfoFactory.dateTypeInfo;
        }
        if (RecordFieldType.TIME.equals(fieldType)) {
            return TypeInfoFactory.intTypeInfo;
        }
        if (RecordFieldType.TIMESTAMP.equals(fieldType)) {
            return TypeInfoFactory.timestampTypeInfo;
        }
        if (RecordFieldType.ARRAY.equals(fieldType)) {
            ArrayDataType arrayDataType = (ArrayDataType) dataType;
            if (RecordFieldType.BYTE.getDataType().equals(arrayDataType.getElementType())) {
                return TypeInfoFactory.getPrimitiveTypeInfo("binary");
            }
            return TypeInfoFactory.getListTypeInfo(getOrcField(arrayDataType.getElementType(), hiveFieldNames));
        }
        if (RecordFieldType.CHOICE.equals(fieldType)) {
            ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            List<DataType> unionFieldSchemas = choiceDataType.getPossibleSubTypes();

            if (unionFieldSchemas != null) {
                // Ignore null types in union
                List<TypeInfo> orcFields = unionFieldSchemas.stream()
                        .map((it) -> NiFiOrcUtils.getOrcField(it, hiveFieldNames))
                        .collect(Collectors.toList());

                // Flatten the field if the union only has one non-null element
                if (orcFields.size() == 1) {
                    return orcFields.get(0);
                } else {
                    return TypeInfoFactory.getUnionTypeInfo(orcFields);
                }
            }
            return null;
        }
        if (RecordFieldType.MAP.equals(fieldType)) {
            MapDataType mapDataType = (MapDataType) dataType;
            return TypeInfoFactory.getMapTypeInfo(
                    getPrimitiveOrcTypeFromPrimitiveFieldType(RecordFieldType.STRING.getDataType()),
                    getOrcField(mapDataType.getValueType(), hiveFieldNames));
        }
        if (RecordFieldType.RECORD.equals(fieldType)) {
            RecordDataType recordDataType = (RecordDataType) dataType;
            List<RecordField> recordFields = recordDataType.getChildSchema().getFields();
            if (recordFields != null) {
                List<String> orcFieldNames = new ArrayList<>(recordFields.size());
                List<TypeInfo> orcFields = new ArrayList<>(recordFields.size());
                recordFields.forEach(recordField -> {
                    String fieldName = hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName();
                    orcFieldNames.add(fieldName);
                    orcFields.add(getOrcField(recordField.getDataType(), hiveFieldNames));
                });
                return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
            }
            return null;
        }

        throw new IllegalArgumentException("Did not recognize field type " + fieldType.name());
    }

    public static TypeInfo getPrimitiveOrcTypeFromPrimitiveFieldType(DataType rawDataType) throws IllegalArgumentException {
        if (rawDataType == null) {
            throw new IllegalArgumentException("Avro type is null");
        }
        RecordFieldType fieldType = rawDataType.getFieldType();
        if (RecordFieldType.INT.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("int");
        }
        if (RecordFieldType.LONG.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("bigint");
        }
        if (RecordFieldType.BOOLEAN.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("boolean");
        }
        if (RecordFieldType.DOUBLE.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("double");
        }
        if (RecordFieldType.FLOAT.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("float");
        }
        if (RecordFieldType.STRING.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("string");
        }

        throw new IllegalArgumentException("Field type " + fieldType.name() + " is not a primitive type");
    }

    public static String getHiveSchema(RecordSchema recordSchema, boolean hiveFieldNames) throws IllegalArgumentException {
        List<RecordField> recordFields = recordSchema.getFields();
        if (recordFields != null) {
            List<String> hiveFields = new ArrayList<>(recordFields.size());
            recordFields.forEach(recordField -> {
                hiveFields.add((hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName())
                        + ":" + getHiveTypeFromFieldType(recordField.getDataType(), hiveFieldNames));
            });
            return "STRUCT<" + StringUtils.join(hiveFields, ", ") + ">";
        }
        return null;
    }

    public static String getHiveTypeFromFieldType(DataType rawDataType, boolean hiveFieldNames) {
        if (rawDataType == null) {
            throw new IllegalArgumentException("Field type is null");
        }
        RecordFieldType dataType = rawDataType.getFieldType();

        if (RecordFieldType.INT.equals(dataType)) {
            return "INT";
        }
        if (RecordFieldType.LONG.equals(dataType)) {
            return "BIGINT";
        }
        if (RecordFieldType.BOOLEAN.equals(dataType)) {
            return "BOOLEAN";
        }
        if (RecordFieldType.DOUBLE.equals(dataType)) {
            return "DOUBLE";
        }
        if (RecordFieldType.FLOAT.equals(dataType)) {
            return "FLOAT";
        }
        if (RecordFieldType.STRING.equals(dataType)) {
            return "STRING";
        }
        if (RecordFieldType.DATE.equals(dataType)) {
            return "DATE";
        }
        if (RecordFieldType.TIME.equals(dataType)) {
            return "INT";
        }
        if (RecordFieldType.TIMESTAMP.equals(dataType)) {
            return "TIMESTAMP";
        }
        if (RecordFieldType.ARRAY.equals(dataType)) {
            ArrayDataType arrayDataType = (ArrayDataType) rawDataType;
            if (RecordFieldType.BYTE.getDataType().equals(arrayDataType.getElementType())) {
                return "BINARY";
            }
            return "ARRAY<" + getHiveTypeFromFieldType(arrayDataType.getElementType(), hiveFieldNames) + ">";
        }
        if (RecordFieldType.MAP.equals(dataType)) {
            MapDataType mapDataType = (MapDataType) rawDataType;
            return "MAP<STRING, " + getHiveTypeFromFieldType(mapDataType.getValueType(), hiveFieldNames) + ">";
        }
        if (RecordFieldType.CHOICE.equals(dataType)) {
            ChoiceDataType choiceDataType = (ChoiceDataType) rawDataType;
            List<DataType> unionFieldSchemas = choiceDataType.getPossibleSubTypes();

            if (unionFieldSchemas != null) {
                // Ignore null types in union
                List<String> hiveFields = unionFieldSchemas.stream()
                        .map((it) -> getHiveTypeFromFieldType(it, hiveFieldNames))
                        .collect(Collectors.toList());

                // Flatten the field if the union only has one non-null element
                return (hiveFields.size() == 1)
                        ? hiveFields.get(0)
                        : "UNIONTYPE<" + StringUtils.join(hiveFields, ", ") + ">";
            }
            return null;
        }

        if (RecordFieldType.RECORD.equals(dataType)) {
            RecordDataType recordDataType = (RecordDataType) rawDataType;
            List<RecordField> recordFields = recordDataType.getChildSchema().getFields();
            if (recordFields != null) {
                List<String> hiveFields = recordFields.stream().map(
                        recordField -> ("`" + (hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName()) + "`:"
                                + getHiveTypeFromFieldType(recordField.getDataType(), hiveFieldNames))).collect(Collectors.toList());
                return "STRUCT<" + StringUtils.join(hiveFields, ", ") + ">";
            }
            return null;
        }

        throw new IllegalArgumentException("Error converting Avro type " + dataType.name() + " to Hive type");
    }


    public static Writer createWriter(
            Path path,
            Configuration conf,
            TypeInfo orcSchema,
            long stripeSize,
            CompressionKind compress,
            int bufferSize) throws IOException {

        int rowIndexStride = (int) OrcConf.ROW_INDEX_STRIDE.getLong(conf);

        boolean addBlockPadding = OrcConf.BLOCK_PADDING.getBoolean(conf);

        String versionName = OrcConf.WRITE_FORMAT.getString(conf);
        OrcFile.Version versionValue = (versionName == null)
                ? OrcFile.Version.CURRENT
                : OrcFile.Version.byName(versionName);

        OrcFile.EncodingStrategy encodingStrategy;
        String enString = OrcConf.ENCODING_STRATEGY.getString(conf);
        if (enString == null) {
            encodingStrategy = OrcFile.EncodingStrategy.SPEED;
        } else {
            encodingStrategy = OrcFile.EncodingStrategy.valueOf(enString);
        }

        final double paddingTolerance = OrcConf.BLOCK_PADDING_TOLERANCE.getDouble(conf);

        long blockSizeValue = OrcConf.BLOCK_SIZE.getLong(conf);

        double bloomFilterFpp = OrcConf.BLOOM_FILTER_FPP.getDouble(conf);

        ObjectInspector inspector = OrcStruct.createObjectInspector(orcSchema);

        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
                .rowIndexStride(rowIndexStride)
                .blockPadding(addBlockPadding)
                .version(versionValue)
                .encodingStrategy(encodingStrategy)
                .paddingTolerance(paddingTolerance)
                .blockSize(blockSizeValue)
                .bloomFilterFpp(bloomFilterFpp)
                .memory(getMemoryManager(conf))
                .inspector(inspector)
                .stripeSize(stripeSize)
                .bufferSize(bufferSize)
                .compress(compress);

        return OrcFile.createWriter(path, writerOptions);
    }

    private static MemoryManager memoryManager = null;

    private static synchronized MemoryManager getMemoryManager(Configuration conf) {
        if (memoryManager == null) {
            memoryManager = new MemoryManagerImpl(conf);
        }
        return memoryManager;
    }
}