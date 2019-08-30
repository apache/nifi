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
package org.apache.hive.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.TimestampParser;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NiFiRecordSerDe extends AbstractSerDe {

    protected RecordReader recordReader;
    protected ComponentLog log;
    protected List<String> columnNames;
    protected StructTypeInfo schema;
    protected SerDeStats stats;

    protected StandardStructObjectInspector cachedObjectInspector;
    protected TimestampParser tsParser;

    private final static Pattern INTERNAL_PATTERN = Pattern.compile("_col([0-9]+)");

    public NiFiRecordSerDe(RecordReader recordReader, ComponentLog log) {
        this.recordReader = recordReader;
        this.log = log;
    }

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        List<TypeInfo> columnTypes;
        StructTypeInfo rowTypeInfo;

        log.debug("Initializing NiFiRecordSerDe: {}", tbl.entrySet().toArray());

        // Get column names and types
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
                .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
        // all table column names
        if (columnNameProperty.isEmpty()) {
            columnNames = new ArrayList<>(0);
        } else {
            columnNames = new ArrayList<>(Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
        }

        // all column types
        if (columnTypeProperty.isEmpty()) {
            columnTypes = new ArrayList<>(0);
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        log.debug("columns: {}, {}", new Object[]{columnNameProperty, columnNames});
        log.debug("types: {}, {} ", new Object[]{columnTypeProperty, columnTypes});

        assert (columnNames.size() == columnTypes.size());

        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        schema = rowTypeInfo;
        log.debug("schema : {}", new Object[]{schema});
        cachedObjectInspector = (StandardStructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        tsParser = new TimestampParser(HiveStringUtils.splitAndUnEscape(tbl.getProperty(serdeConstants.TIMESTAMP_FORMATS)));
        stats = new SerDeStats();
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ObjectWritable.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        throw new UnsupportedOperationException("This SerDe only supports deserialization");
    }

    @Override
    public SerDeStats getSerDeStats() {
        return stats;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        ObjectWritable t = (ObjectWritable) writable;
        Record record = (Record) t.get();
        List<Object> r = new ArrayList<>(Collections.nCopies(columnNames.size(), null));
        try {
            RecordSchema recordSchema = record.getSchema();
            for (RecordField field : recordSchema.getFields()) {
                populateRecord(r, record.getValue(field), field, schema);
            }
            stats.setRowCount(stats.getRowCount() + 1);

        } catch(SerDeException se) {
            log.error("Error [{}] parsing Record [{}].", new Object[]{se.toString(), t}, se);
            throw se;
        } catch (Exception e) {
            log.error("Error [{}] parsing Record [{}].", new Object[]{e.toString(), t}, e);
            throw new SerDeException(e);
        }

        return r;
    }

    @SuppressWarnings("unchecked")
    private Object extractCurrentField(final Object fieldValue, final RecordField field, final TypeInfo fieldTypeInfo) throws SerDeException {
        if(fieldValue == null){
            return null;
        }

        Object val;
        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN;
                if (fieldTypeInfo instanceof PrimitiveTypeInfo) {
                    primitiveCategory = ((PrimitiveTypeInfo) fieldTypeInfo).getPrimitiveCategory();
                }
                switch (primitiveCategory) {
                    case BYTE:
                        Integer bIntValue = DataTypeUtils.toInteger(fieldValue, field.getFieldName());
                        val = bIntValue.byteValue();
                        break;
                    case SHORT:
                        Integer sIntValue = DataTypeUtils.toInteger(fieldValue, field.getFieldName());
                        val = sIntValue.shortValue();
                        break;
                    case INT:
                        val = DataTypeUtils.toInteger(fieldValue, field.getFieldName());
                        break;
                    case LONG:
                        val = DataTypeUtils.toLong(fieldValue, field.getFieldName());
                        break;
                    case BOOLEAN:
                        val = DataTypeUtils.toBoolean(fieldValue, field.getFieldName());
                        break;
                    case FLOAT:
                        val = DataTypeUtils.toFloat(fieldValue, field.getFieldName());
                        break;
                    case DOUBLE:
                        val = DataTypeUtils.toDouble(fieldValue, field.getFieldName());
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        val = DataTypeUtils.toString(fieldValue, field.getFieldName());
                        break;
                    case BINARY:
                        final ArrayDataType arrayDataType;
                        if(fieldValue instanceof String) {
                            // Treat this as an array of bytes
                            arrayDataType = (ArrayDataType) RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
                        } else {
                            arrayDataType = (ArrayDataType) field.getDataType();
                        }
                        Object[] array = DataTypeUtils.toArray(fieldValue, field.getFieldName(), arrayDataType.getElementType());
                        val = AvroTypeUtil.convertByteArray(array).array();
                        break;
                    case DATE:
                        Date d = DataTypeUtils.toDate(fieldValue, () -> DataTypeUtils.getDateFormat(field.getDataType().getFormat()), field.getFieldName());
                        org.apache.hadoop.hive.common.type.Date hiveDate = new org.apache.hadoop.hive.common.type.Date();
                        hiveDate.setTimeInMillis(d.getTime());
                        val = hiveDate;
                        break;
                    // ORC doesn't currently handle TIMESTAMPLOCALTZ
                    case TIMESTAMP:
                        Timestamp ts = DataTypeUtils.toTimestamp(fieldValue, () -> DataTypeUtils.getDateFormat(field.getDataType().getFormat()), field.getFieldName());;
                        // Convert to Hive's Timestamp type
                        org.apache.hadoop.hive.common.type.Timestamp hivetimestamp = new org.apache.hadoop.hive.common.type.Timestamp();
                        hivetimestamp.setTimeInMillis(ts.getTime(), ts.getNanos());
                        val = hivetimestamp;
                        break;
                    case DECIMAL:
                        if(fieldValue instanceof BigDecimal){
                            val = HiveDecimal.create((BigDecimal) fieldValue);
                        } else if (fieldValue instanceof Double){
                            val = HiveDecimal.create((Double)fieldValue);
                        } else if (fieldValue instanceof Number) {
                            val = HiveDecimal.create(((Number)fieldValue).doubleValue());
                        } else {
                            val = HiveDecimal.create(DataTypeUtils.toDouble(fieldValue, field.getDataType().getFormat()));
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Field " + field.getFieldName() + " cannot be converted to type: " + primitiveCategory.name());
                }
                break;
            case LIST:
                Object[] value = (Object[])fieldValue;
                ListTypeInfo listTypeInfo = (ListTypeInfo)fieldTypeInfo;
                TypeInfo nestedType = listTypeInfo.getListElementTypeInfo();
                List<Object> converted = new ArrayList<>(value.length);
                for(int i=0; i<value.length; i++){
                    converted.add(extractCurrentField(value[i], field, nestedType));
                }
                val = converted;
                break;
            case MAP:
                //in nifi all maps are <String,?> so use that
                Map<String, Object> valueMap = (Map<String,Object>)fieldValue;
                MapTypeInfo mapTypeInfo = (MapTypeInfo)fieldTypeInfo;
                Map<Object, Object> convertedMap = new HashMap<>(valueMap.size());
                //get a key record field, nifi map keys are always string. synthesize new
                //record fields for the map field key and value.
                RecordField keyField = new RecordField(field.getFieldName() + ".key", RecordFieldType.STRING.getDataType());
                RecordField valueField = new RecordField(field.getFieldName() + ".value", ((MapDataType)field.getDataType()).getValueType());
                for (Map.Entry<String, Object> entry: valueMap.entrySet()) {
                    convertedMap.put(
                            extractCurrentField(entry.getKey(), keyField, mapTypeInfo.getMapKeyTypeInfo()),
                            extractCurrentField(entry.getValue(), valueField, mapTypeInfo.getMapValueTypeInfo())
                    );
                }
                val = convertedMap;
                break;
            case STRUCT:
                Record nestedRecord = (Record) fieldValue;
                StructTypeInfo s = (StructTypeInfo) fieldTypeInfo;
                int numNestedRecordFields = s.getAllStructFieldTypeInfos().size();
                List<Object> struct = new ArrayList<>(Collections.nCopies(numNestedRecordFields, null));
                try {
                    RecordSchema recordSchema = nestedRecord.getSchema();
                    for (RecordField nestedRecordField : recordSchema.getFields()) {
                        populateRecord(struct, nestedRecord.getValue(nestedRecordField), nestedRecordField, s);
                    }
                    val = struct;
                } catch (Exception e) {
                    log.error("Error [{}] parsing child record [{}].", new Object[]{e.toString(), nestedRecord}, e);
                    throw new SerDeException(e);
                }
                break;
            default:
                log.error("Unknown type found: " + fieldTypeInfo + "for field of type: " + field.getDataType().toString());
                return null;
        }
        return val;
    }



    @Override
    public ObjectInspector getObjectInspector() {
        return cachedObjectInspector;
    }



    private void populateRecord(List<Object> r, Object value, RecordField field, StructTypeInfo typeInfo) throws SerDeException {

        String fieldName = field.getFieldName();
        String normalizedFieldName = fieldName.toLowerCase();

        // Normalize struct field names and search for the specified (normalized) field name
        int fpos = typeInfo.getAllStructFieldNames().stream().map((s) -> s == null ? null : s.toLowerCase()).collect(Collectors.toList()).indexOf(normalizedFieldName);
        if (fpos == -1) {
            Matcher m = INTERNAL_PATTERN.matcher(fieldName);
            fpos = m.matches() ? Integer.parseInt(m.group(1)) : -1;

            log.debug("NPE finding position for field [{}] in schema [{}],"
                    + " attempting to check if it is an internal column name like _col0", new Object[]{fieldName, typeInfo});
            if (fpos == -1) {
                // unknown field, we return. We'll continue from the next field onwards. Log at debug level because partition columns will be "unknown fields"
                log.debug("Field {} is not found in the target table, ignoring...", new Object[]{field.getFieldName()});
                return;
            }
            // If we get past this, then the column name did match the hive pattern for an internal
            // column name, such as _col0, etc, so it *MUST* match the schema for the appropriate column.
            // This means people can't use arbitrary column names such as _col0, and expect us to ignore it
            // if we find it.
            if (!fieldName.equalsIgnoreCase(HiveConf.getColumnInternalName(fpos))) {
                log.error("Hive internal column name {} and position "
                        + "encoding {} for the column name are at odds", new Object[]{fieldName, fpos});
                throw new SerDeException("Hive internal column name (" + fieldName
                        + ") and position encoding (" + fpos
                        + ") for the column name are at odds");
            }
            // If we reached here, then we were successful at finding an alternate internal
            // column mapping, and we're about to proceed.
        }
        Object currField = extractCurrentField(value, field, typeInfo.getStructFieldTypeInfo(normalizedFieldName));
        r.set(fpos, currField);
    }

}
