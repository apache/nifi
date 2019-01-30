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
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
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

public class NiFiRecordSerDe extends AbstractSerDe {

    protected RecordReader recordReader;
    protected ComponentLog log;
    protected List<String> columnNames;
    protected StructTypeInfo schema;
    protected SerDeStats stats;

    protected StandardStructObjectInspector cachedObjectInspector;
    protected TimestampParser tsParser;

    private final static Pattern INTERNAL_PATTERN = Pattern.compile("_col([0-9]+)");

    private Map<String, Integer> fieldPositionMap;

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
        // Populate mapping of field names to column positions
        try {
            populateFieldPositionMap();
        } catch (MalformedRecordException | IOException e) {
            throw new SerDeException(e);
        }
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
                String fieldName = field.getFieldName();
                String normalizedFieldName = fieldName.toLowerCase();

                // Get column position of field name, and set field value there
                Integer fpos = fieldPositionMap.get(normalizedFieldName);
                if(fpos == null || fpos == -1) {
                    // This is either a partition column or not a column in the target table, ignore either way
                    continue;
                }
                Object currField = extractCurrentField(record, field, schema.getStructFieldTypeInfo(normalizedFieldName));
                r.set(fpos, currField);
            }
            stats.setRowCount(stats.getRowCount() + 1);

        } catch (Exception e) {
            log.warn("Error [{}] parsing Record [{}].", new Object[]{e.toString(), t}, e);
            throw new SerDeException(e);
        }

        return r;
    }

    /**
     * Utility method to extract current expected field from given record.
     */
    @SuppressWarnings("unchecked")
    private Object extractCurrentField(Record record, RecordField field, TypeInfo fieldTypeInfo) throws SerDeException {
        Object val;
        if (field == null) {
            return null;
        }
        String fieldName = field.getFieldName();

        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN;
                if (fieldTypeInfo instanceof PrimitiveTypeInfo) {
                    primitiveCategory = ((PrimitiveTypeInfo) fieldTypeInfo).getPrimitiveCategory();
                }
                switch (primitiveCategory) {
                    case BYTE:
                        Integer bIntValue = record.getAsInt(fieldName);
                        val = bIntValue == null ? null : bIntValue.byteValue();
                        break;
                    case SHORT:
                        Integer sIntValue = record.getAsInt(fieldName);
                        val = sIntValue == null ? null : sIntValue.shortValue();
                        break;
                    case INT:
                        val = record.getAsInt(fieldName);
                        break;
                    case LONG:
                        val = record.getAsLong(fieldName);
                        break;
                    case BOOLEAN:
                        val = record.getAsBoolean(fieldName);
                        break;
                    case FLOAT:
                        val = record.getAsFloat(fieldName);
                        break;
                    case DOUBLE:
                        val = record.getAsDouble(fieldName);
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        val = record.getAsString(fieldName);
                        break;
                    case BINARY:
                        Object[] array = record.getAsArray(fieldName);
                        if (array == null) {
                            return null;
                        }
                        val = AvroTypeUtil.convertByteArray(array).array();
                        break;
                    case DATE:
                        Date d = record.getAsDate(fieldName, field.getDataType().getFormat());
                        if(d != null) {
                            org.apache.hadoop.hive.common.type.Date hiveDate = new org.apache.hadoop.hive.common.type.Date();
                            hiveDate.setTimeInMillis(d.getTime());
                            val = hiveDate;
                        } else {
                            val = null;
                        }
                        break;
                    // ORC doesn't currently handle TIMESTAMPLOCALTZ
                    case TIMESTAMP:
                        Timestamp ts = DataTypeUtils.toTimestamp(record.getValue(fieldName), () -> DataTypeUtils.getDateFormat(field.getDataType().getFormat()), fieldName);
                        if(ts != null) {
                            // Convert to Hive's Timestamp type
                            org.apache.hadoop.hive.common.type.Timestamp hivetimestamp = new org.apache.hadoop.hive.common.type.Timestamp();
                            hivetimestamp.setTimeInMillis(ts.getTime(), ts.getNanos());
                            val = hivetimestamp;
                        } else {
                            val = null;
                        }
                        break;
                    case DECIMAL:
                        Double value = record.getAsDouble(fieldName);
                        val = value == null ? null : HiveDecimal.create(value);
                        break;
                    default:
                        throw new IllegalArgumentException("Field " + fieldName + " cannot be converted to type: " + primitiveCategory.name());
                }
                break;
            case LIST:
                Object[] value = record.getAsArray(fieldName);
                val = value == null ? null : Arrays.asList(value);
                break;
            case MAP:
                val = record.getValue(fieldName);
                break;
            case STRUCT:
                // The Hive StandardStructObjectInspector expects the object corresponding to a "struct" to be an array or List rather than a Map.
                // Do the conversion here, calling extractCurrentField recursively to traverse any nested structs.
                Record nestedRecord = (Record) record.getValue(fieldName);
                if (nestedRecord == null) {
                    return null;
                }
                try {
                    RecordSchema recordSchema = nestedRecord.getSchema();
                    List<RecordField> recordFields = recordSchema.getFields();
                    if (recordFields == null || recordFields.isEmpty()) {
                        return Collections.emptyList();
                    }
                    // This List will hold the values of the entries in the Map
                    List<Object> structList = new ArrayList<>(recordFields.size());
                    StructTypeInfo typeInfo = (StructTypeInfo) schema.getStructFieldTypeInfo(fieldName);
                    for (RecordField nestedRecordField : recordFields) {
                        String fName = nestedRecordField.getFieldName();
                        String normalizedFieldName = fName.toLowerCase();
                        structList.add(extractCurrentField(nestedRecord, nestedRecordField, typeInfo.getStructFieldTypeInfo(normalizedFieldName)));
                    }
                    return structList;
                } catch (Exception e) {
                    log.warn("Error [{}] parsing Record [{}].", new Object[]{e.toString(), nestedRecord}, e);
                    throw new SerDeException(e);
                }
                // break unreachable
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

    private void populateFieldPositionMap() throws MalformedRecordException, IOException {
        // Populate the mapping of field names to column positions only once
        fieldPositionMap = new HashMap<>(columnNames.size());

        RecordSchema recordSchema = recordReader.getSchema();
        for (RecordField field : recordSchema.getFields()) {
            String fieldName = field.getFieldName();
            String normalizedFieldName = fieldName.toLowerCase();

            int fpos = schema.getAllStructFieldNames().indexOf(fieldName.toLowerCase());
            if (fpos == -1) {
                Matcher m = INTERNAL_PATTERN.matcher(fieldName);
                fpos = m.matches() ? Integer.parseInt(m.group(1)) : -1;

                log.debug("NPE finding position for field [{}] in schema [{}],"
                        + " attempting to check if it is an internal column name like _col0", new Object[]{fieldName, schema});
                if (fpos == -1) {
                    // unknown field, we return. We'll continue from the next field onwards. Log at debug level because partition columns will be "unknown fields"
                    log.debug("Field {} is not found in the target table, ignoring...", new Object[]{field.getFieldName()});
                    continue;
                }
                // If we get past this, then the column name did match the hive pattern for an internal
                // column name, such as _col0, etc, so it *MUST* match the schema for the appropriate column.
                // This means people can't use arbitrary column names such as _col0, and expect us to ignore it
                // if we find it.
                if (!fieldName.equalsIgnoreCase(HiveConf.getColumnInternalName(fpos))) {
                    log.error("Hive internal column name {} and position "
                            + "encoding {} for the column name are at odds", new Object[]{fieldName, fpos});
                    throw new IOException("Hive internal column name (" + fieldName
                            + ") and position encoding (" + fpos
                            + ") for the column name are at odds");
                }
                // If we reached here, then we were successful at finding an alternate internal
                // column mapping, and we're about to proceed.
            }
            fieldPositionMap.put(normalizedFieldName, fpos);
        }
    }
}
