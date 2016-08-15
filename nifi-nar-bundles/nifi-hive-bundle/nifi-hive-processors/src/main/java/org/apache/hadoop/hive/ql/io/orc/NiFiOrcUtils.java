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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_BLOCK_PADDING;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_BLOCK_SIZE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_WRITE_FORMAT;

/**
 * Utility methods for ORC support (conversion from Avro, conversion to Hive types, e.g.
 */
public class NiFiOrcUtils {

    public static Object convertToORCObject(TypeInfo typeInfo, Object o) {
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
                    union.set((byte) index, convertToORCObject(objectTypeInfo, o));
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
            if (o instanceof String || o instanceof Utf8 || o instanceof GenericData.EnumSymbol) {
                return new Text(o.toString());
            }
            if (o instanceof ByteBuffer) {
                return new BytesWritable(((ByteBuffer) o).array());
            }
            if (o instanceof int[]) {
                int[] intArray = (int[]) o;
                return Arrays.stream(intArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("int"), element))
                        .collect(Collectors.toList());
            }
            if (o instanceof long[]) {
                long[] longArray = (long[]) o;
                return Arrays.stream(longArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("bigint"), element))
                        .collect(Collectors.toList());
            }
            if (o instanceof float[]) {
                float[] floatArray = (float[]) o;
                return IntStream.range(0, floatArray.length)
                        .mapToDouble(i -> floatArray[i])
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("float"), (float) element))
                        .collect(Collectors.toList());
            }
            if (o instanceof double[]) {
                double[] doubleArray = (double[]) o;
                return Arrays.stream(doubleArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("double"), element))
                        .collect(Collectors.toList());
            }
            if (o instanceof boolean[]) {
                boolean[] booleanArray = (boolean[]) o;
                return IntStream.range(0, booleanArray.length)
                        .map(i -> booleanArray[i] ? 1 : 0)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("boolean"), element == 1))
                        .collect(Collectors.toList());
            }
            if (o instanceof GenericData.Array) {
                GenericData.Array array = ((GenericData.Array) o);
                // The type information in this case is interpreted as a List
                TypeInfo listTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
                return array.stream().map((element) -> convertToORCObject(listTypeInfo, element)).collect(Collectors.toList());
            }
            if (o instanceof List) {
                return o;
            }
            if (o instanceof Map) {
                MapWritable mapWritable = new MapWritable();
                TypeInfo keyInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
                TypeInfo valueInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
                // Unions are not allowed as key/value types, so if we convert the key and value objects,
                // they should return Writable objects
                ((Map) o).forEach((key, value) -> {
                    Object keyObject = convertToORCObject(keyInfo, key);
                    Object valueObject = convertToORCObject(valueInfo, value);
                    if (keyObject == null
                            || !(keyObject instanceof Writable)
                            || !(valueObject instanceof Writable)
                            ) {
                        throw new IllegalArgumentException("Maps may only contain Writable types, and the key cannot be null");
                    }
                    mapWritable.put((Writable) keyObject, (Writable) valueObject);
                });
                return mapWritable;
            }

        }
        return null;
    }


    /**
     * Create an object of OrcStruct given a TypeInfo and a list of objects
     *
     * @param typeInfo The TypeInfo object representing the ORC record schema
     * @param objs     ORC objects/Writables
     * @return an OrcStruct containing the specified objects for the specified schema
     */
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


    public static TypeInfo getOrcField(Schema fieldSchema) throws IllegalArgumentException {
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

                if (unionFieldSchemas != null) {
                    // Ignore null types in union
                    List<TypeInfo> orcFields = unionFieldSchemas.stream().filter(
                            unionFieldSchema -> !Schema.Type.NULL.equals(unionFieldSchema.getType()))
                            .map(NiFiOrcUtils::getOrcField)
                            .collect(Collectors.toList());

                    // Flatten the field if the union only has one non-null element
                    if (orcFields.size() == 1) {
                        return orcFields.get(0);
                    } else {
                        return TypeInfoFactory.getUnionTypeInfo(orcFields);
                    }
                }
                return null;

            case ARRAY:
                return TypeInfoFactory.getListTypeInfo(getOrcField(fieldSchema.getElementType()));

            case MAP:
                return TypeInfoFactory.getMapTypeInfo(
                        getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING),
                        getOrcField(fieldSchema.getValueType()));

            case RECORD:
                List<Schema.Field> avroFields = fieldSchema.getFields();
                if (avroFields != null) {
                    List<String> orcFieldNames = new ArrayList<>(avroFields.size());
                    List<TypeInfo> orcFields = new ArrayList<>(avroFields.size());
                    avroFields.forEach(avroField -> {
                        String fieldName = avroField.name();
                        orcFieldNames.add(fieldName);
                        orcFields.add(getOrcField(avroField.schema()));
                    });
                    return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
                }
                return null;

            case ENUM:
                // An enum value is just a String for ORC/Hive
                return getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING);

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

    public static TypeInfo getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type avroType) throws IllegalArgumentException {
        if (avroType == null) {
            throw new IllegalArgumentException("Avro type is null");
        }
        switch (avroType) {
            case INT:
                return TypeInfoFactory.getPrimitiveTypeInfo("int");
            case LONG:
                return TypeInfoFactory.getPrimitiveTypeInfo("bigint");
            case BOOLEAN:
                return TypeInfoFactory.getPrimitiveTypeInfo("boolean");
            case BYTES:
                return TypeInfoFactory.getPrimitiveTypeInfo("binary");
            case DOUBLE:
                return TypeInfoFactory.getPrimitiveTypeInfo("double");
            case FLOAT:
                return TypeInfoFactory.getPrimitiveTypeInfo("float");
            case STRING:
                return TypeInfoFactory.getPrimitiveTypeInfo("string");
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


    public static OrcFlowFileWriter createWriter(OutputStream flowFileOutputStream,
                                                 Path path,
                                                 Configuration conf,
                                                 TypeInfo orcSchema,
                                                 long stripeSize,
                                                 CompressionKind compress,
                                                 int bufferSize) throws IOException {

        int rowIndexStride = HiveConf.getIntVar(conf, HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE);

        boolean addBlockPadding = HiveConf.getBoolVar(conf, HIVE_ORC_DEFAULT_BLOCK_PADDING);

        String versionName = HiveConf.getVar(conf, HIVE_ORC_WRITE_FORMAT);
        OrcFile.Version versionValue = (versionName == null)
                ? OrcFile.Version.CURRENT
                : OrcFile.Version.byName(versionName);

        OrcFile.EncodingStrategy encodingStrategy;
        String enString = conf.get(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname);
        if (enString == null) {
            encodingStrategy = OrcFile.EncodingStrategy.SPEED;
        } else {
            encodingStrategy = OrcFile.EncodingStrategy.valueOf(enString);
        }

        OrcFile.CompressionStrategy compressionStrategy;
        String compString = conf.get(HiveConf.ConfVars.HIVE_ORC_COMPRESSION_STRATEGY.varname);
        if (compString == null) {
            compressionStrategy = OrcFile.CompressionStrategy.SPEED;
        } else {
            compressionStrategy = OrcFile.CompressionStrategy.valueOf(compString);
        }

        float paddingTolerance;
        paddingTolerance = conf.getFloat(HiveConf.ConfVars.HIVE_ORC_BLOCK_PADDING_TOLERANCE.varname,
                HiveConf.ConfVars.HIVE_ORC_BLOCK_PADDING_TOLERANCE.defaultFloatVal);

        long blockSizeValue = HiveConf.getLongVar(conf, HIVE_ORC_DEFAULT_BLOCK_SIZE);

        double bloomFilterFpp = BloomFilterIO.DEFAULT_FPP;

        ObjectInspector inspector = OrcStruct.createObjectInspector(orcSchema);

        return new OrcFlowFileWriter(flowFileOutputStream,
                path,
                conf,
                inspector,
                stripeSize,
                compress,
                bufferSize,
                rowIndexStride,
                getMemoryManager(conf),
                addBlockPadding,
                versionValue,
                null, // no callback
                encodingStrategy,
                compressionStrategy,
                paddingTolerance,
                blockSizeValue,
                null, // no Bloom Filter column names
                bloomFilterFpp);
    }

    private static MemoryManager memoryManager = null;

    private static synchronized MemoryManager getMemoryManager(Configuration conf) {
        if (memoryManager == null) {
            memoryManager = new MemoryManager(conf);
        }
        return memoryManager;
    }
}