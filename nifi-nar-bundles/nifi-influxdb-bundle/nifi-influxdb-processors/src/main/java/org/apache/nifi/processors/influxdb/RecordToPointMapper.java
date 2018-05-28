/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.influxdb;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.influxdb.WriteOptions.MissingItemsBehaviour;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StopWatch;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class RecordToPointMapper {

    private final ComponentLog log;
    private final ProcessSession session;
    private final ProcessContext context;

    private final WriteOptions options;

    private FlowFile flowFile;
    private RecordSchema schema;

    private StopWatch stopWatch = new StopWatch();
    private List<Point> points = new ArrayList<>();

    private RecordToPointMapper(@NonNull final ProcessSession session,
                                @NonNull final ProcessContext context,
                                @NonNull final ComponentLog log,
                                @NonNull final WriteOptions options) {

        Objects.requireNonNull(session, "ProcessSession is required");
        Objects.requireNonNull(context, "ProcessContext is required");
        Objects.requireNonNull(log, "ComponentLog is required");
        Objects.requireNonNull(options, "WriteOptions is required");

        this.session = session;
        this.context = context;
        this.log = log;
        this.options = options;
    }

    @NonNull
    public static RecordToPointMapper createMapper(@NonNull final ProcessSession session,
                                                   @NonNull final ProcessContext context,
                                                   @NonNull final ComponentLog log,
                                                   @NonNull final WriteOptions options) {
        return new RecordToPointMapper(session, context, log, options);
    }

    @NonNull
    protected RecordToPointMapper mapFlowFile(@Nullable final FlowFile flowFile) throws Exception {


        this.flowFile = flowFile;

        if (flowFile == null) {
            return this;
        }

        try (final InputStream stream = session.read(flowFile)) {

            mapInputStream(stream);
        }

        return this;
    }

    @NonNull
    protected RecordToPointMapper writeToInflux(@NonNull final InfluxDB influxDB) {

        Objects.requireNonNull(flowFile, "FlowFile is required");
        Objects.requireNonNull(points, "Points are required");

        stopWatch.start();

        if (influxDB.isBatchEnabled()) {

            // Write by batching
            points.forEach(point -> influxDB.write(options.getDatabase(), options.getRetentionPolicy(), point));

        } else {

            BatchPoints batch = BatchPoints
                    .database(options.getDatabase())
                    .retentionPolicy(options.getRetentionPolicy())
                    .build();

            points.forEach(batch::point);

            // Write all Points together
            influxDB.write(batch);
        }

        stopWatch.stop();

        return this;
    }

    protected void reportResults(@Nullable final String url) {

        Objects.requireNonNull(flowFile, "FlowFile is required");

        if (!points.isEmpty()) {

            String transitUri = url + "/" + options.getDatabase();
            String message = String.format("Added %d points to InfluxDB.", points.size());

            session.getProvenanceReporter().send(flowFile, transitUri, message, stopWatch.getElapsed(MILLISECONDS));
        } else {

            String flowFileName = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());

            log.info("The all fields of FlowFile={} has null value. There is nothing to store to InfluxDB",
                    new Object[]{flowFileName});
        }
    }

    private void mapInputStream(@NonNull final InputStream stream) throws
            IOException, MalformedRecordException, SchemaNotFoundException {

        Objects.requireNonNull(flowFile, "FlowFile is required");

        RecordReaderFactory factory = context
                .getProperty(PutInfluxDBRecord.RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        RecordReader parser = factory
                .createRecordReader(flowFile, stream, log);

        schema = parser.getSchema();

        Record record;
        while ((record = parser.nextRecord()) != null) {

            mapRecord(record);
        }
    }

    private void mapRecord(@Nullable final Record record) throws MalformedRecordException {

        if (record == null) {
            return;
        }

        // If the Record contains a field with measurement property value,
        // then value of the Record field is use as InfluxDB measurement.
        String measurement = record.getAsString(options.getMeasurement());
        if (StringUtils.isBlank(measurement)) {

            measurement = options.getMeasurement();
        }

        Point.Builder point = Point.measurement(measurement);

        mapFields(record, point);

        if (point.hasFields()) {

            mapTags(record, point);
            mapTimestamp(record, point);

            points.add(point.build());
        }
    }

    private void mapFields(@Nullable final Record record, @NonNull final Point.Builder point) {

        Objects.requireNonNull(point, "Point is required");

        if (record == null) {
            return;
        }

        RecordFieldMapper recordFieldMapper = new RecordFieldMapper(record, point);

        options.getFields().forEach(fieldName -> {

            RecordField recordField = findRecordField(fieldName, options.getMissingFields());
            if (recordField != null) {

                recordFieldMapper.apply(recordField);
            }
        });
    }

    private void mapTags(@Nullable final Record record, @NonNull final Point.Builder point) {

        Objects.requireNonNull(point, "Point is required");

        if (record == null) {
            return;
        }

        SaveKeyValue saveTag = (key, value, dataType) -> {

            Objects.requireNonNull(key, "Tag Key is required");
            Objects.requireNonNull(dataType, "Tag Value Type is required");

            String stringValue = DataTypeUtils.toString(value, dataType.getFormat());
            if (stringValue == null) {
                return;
            }

            point.tag(key, stringValue);
        };


        for (String tag : options.getTags()) {

            RecordField recordField = findRecordField(tag, options.getMissingTags());
            if (recordField == null) {
                continue;
            }

            Object recordValue = record.getValue(recordField);
            if (recordValue == null) {
                nullBehaviour(tag);
                continue;
            }

            DataType dataType = recordField.getDataType();

            mapTag(tag, recordValue, dataType, saveTag);
        }
    }

    private void mapTimestamp(@Nullable final Record record, @NonNull final Point.Builder point) {

        Objects.requireNonNull(point, "Point is required");

        if (record == null) {
            return;
        }

        RecordField recordField = findRecordField(options.getTimestamp(), MissingItemsBehaviour.IGNORE);
        if (recordField == null) {
            return;
        }

        Object value = record.getValue(recordField);
        if (value == null) {
            return;
        }

        Long time;
        TimeUnit precision;

        time = DataTypeUtils.toLong(value, recordField.getFieldName());
        if (value instanceof Date) {
            precision = MILLISECONDS;
        } else {
            precision = options.getPrecision();
        }

        point.time(time, precision);
    }

    private void mapTag(@NonNull final String tag,
                        @Nullable final Object value,
                        @NonNull final DataType type,
                        @NonNull final SaveKeyValue saveKeyValue) {

        Objects.requireNonNull(tag, "Tag is required");
        Objects.requireNonNull(type, "DataType is required");
        Objects.requireNonNull(saveKeyValue, "SaveKeyValue is required");

        RecordFieldType defaultDataType = RecordFieldType.STRING;

        switch (type.getFieldType()) {

            case STRING:
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case SHORT:
            case INT:
            case BIGINT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIMESTAMP:
                saveKeyValue.save(tag, value, type);
                break;

            case CHOICE:
                DataType choiceValueType = DataTypeUtils.chooseDataType(value, (ChoiceDataType) type);
                if (choiceValueType == null) {
                    choiceValueType = defaultDataType.getDataType();
                }

                Object choiceValue = DataTypeUtils.convertType(value, choiceValueType, tag);

                mapTag(tag, choiceValue, choiceValueType, saveKeyValue);
                break;

            case MAP:

                DataType mapValueType = ((MapDataType) type).getValueType();
                if (mapValueType == null) {
                    mapValueType = defaultDataType.getDataType();
                }

                Map<String, Object> map = DataTypeUtils.toMap(value, tag);
                storeMap(map, mapValueType, saveKeyValue);

                break;

            case ARRAY:
            case RECORD:
                handleComplexField(value, type, tag, saveKeyValue);
                break;

            default:
                unsupportedType(type, tag);
        }
    }

    private void storeMap(@Nullable final Map<String, ?> map,
                          @NonNull final DataType mapValueType,
                          @NonNull final SaveKeyValue saveKeyValue) {

        Objects.requireNonNull(mapValueType, "MapValueType is required");
        Objects.requireNonNull(saveKeyValue, "SaveKeyValue function is required");

        if (map == null) {
            return;
        }

        for (final String mapKey : map.keySet()) {

            Object rawValue = map.get(mapKey);
            DataType rawType = mapValueType;

            if (mapValueType.getFieldType() == RecordFieldType.CHOICE) {

                rawType = DataTypeUtils.chooseDataType(rawValue, (ChoiceDataType) mapValueType);
            }

            if (rawType == null) {
                String message = String.format("Map does not have a specified type of values. MapValueType: %s", mapValueType);

                throw new IllegalStateException(message);
            }

            Object mapValue = DataTypeUtils.convertType(rawValue, rawType, mapKey);
            saveKeyValue.save(mapKey, mapValue, rawType);
        }
    }

    @Nullable
    private RecordField findRecordField(@Nullable final String fieldName,
                                        @NonNull final MissingItemsBehaviour missingBehaviour) {

        Objects.requireNonNull(missingBehaviour, "MissingItemsBehaviour for not defined items is required");

        RecordField recordField = schema.getField(fieldName).orElse(null);

        // missing field + FAIL => exception
        if (recordField == null && MissingItemsBehaviour.FAIL.equals(missingBehaviour)) {

            String message = String.format(PutInfluxDBRecord.REQUIRED_FIELD_MISSING, fieldName);

            throw new IllegalStateException(message);
        }

        return recordField;
    }

    private void unsupportedType(@NonNull final DataType dataType, @NonNull final String fieldName) {

        Objects.requireNonNull(fieldName, "fieldName is required");
        Objects.requireNonNull(dataType, "DataType is required");

        RecordFieldType fieldType = dataType.getFieldType();

        String message = String.format(PutInfluxDBRecord.UNSUPPORTED_FIELD_TYPE, fieldName, fieldType);

        throw new IllegalStateException(message);
    }

    private void handleComplexField(@Nullable final Object value,
                                    @NonNull final DataType fieldType,
                                    @NonNull final String fieldName,
                                    @NonNull final SaveKeyValue saveKeyValue) {

        Objects.requireNonNull(fieldType, "RecordFieldType is required.");
        Objects.requireNonNull(fieldName, "FieldName is required.");

        switch (options.getComplexFieldBehaviour()) {

            case FAIL:
                String message = String.format("Complex value found for %s; routing to failure", fieldName);
                log.error(message);
                throw new IllegalStateException(message);

            case WARN:
                log.warn("Complex value found for {}; skipping", new Object[]{fieldName});
                break;

            case TEXT:
                String stringValue = DataTypeUtils.toString(value, fieldType.getFormat());

                saveKeyValue.save(fieldName, stringValue, RecordFieldType.STRING.getDataType());
                break;
            case IGNORE:
                // skip
                break;
        }
    }

    private interface SaveKeyValue {
        void save(@NonNull final String key, @Nullable final Object value, @NonNull final DataType dataType);
    }

    private final class RecordFieldMapper {

        private final Record record;
        private final Point.Builder point;

        private RecordFieldMapper(@NonNull final Record record,
                                  @NonNull final Point.Builder point) {
            this.record = record;
            this.point = point;
        }

        public void apply(@NonNull final RecordField recordField) {

            String field = recordField.getFieldName();
            Object value = record.getValue(recordField);

            addField(field, value, recordField.getDataType());
        }

        private void addField(@NonNull final String field,
                              @Nullable final Object value,
                              @NonNull final DataType dataType) {

            if (value == null) {
                nullBehaviour(field);
                return;
            }

            DataType defaultValueType = RecordFieldType.CHOICE.getChoiceDataType(
                    RecordFieldType.FLOAT.getDataType(),
                    RecordFieldType.LONG.getDataType(),
                    RecordFieldType.BOOLEAN.getDataType(),
                    RecordFieldType.STRING.getDataType());

            switch (dataType.getFieldType()) {

                case DATE:
                case TIME:
                case TIMESTAMP:
                    Long time = DataTypeUtils.toLong(value, field);
                    point.addField(field, time);
                    break;

                case DOUBLE:
                    Double doubleNumber = DataTypeUtils.toDouble(value, field);
                    point.addField(field, doubleNumber);
                    break;

                case FLOAT:
                    Float floatNumber = DataTypeUtils.toFloat(value, field);
                    point.addField(field, floatNumber);
                    break;

                case LONG:
                    Long longNumber = DataTypeUtils.toLong(value, field);
                    point.addField(field, longNumber);
                    break;

                case INT:
                case BYTE:
                case SHORT:
                    Integer integerNumber = DataTypeUtils.toInteger(value, field);
                    point.addField(field, integerNumber);
                    break;

                case BIGINT:
                    BigInteger bigIntegerNumber = DataTypeUtils.toBigInt(value, field);
                    point.addField(field, bigIntegerNumber);
                    break;

                case BOOLEAN:
                    Boolean bool = DataTypeUtils.toBoolean(value, field);
                    point.addField(field, bool);
                    break;

                case CHAR:
                case STRING:
                    String stringValue = value.toString();
                    point.addField(field, stringValue);
                    break;

                case CHOICE:
                    DataType choiceDataType = DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType);
                    if (choiceDataType == null) {
                        choiceDataType = defaultValueType;
                    }

                    Object choiceValue = DataTypeUtils.convertType(value, choiceDataType, field);

                    //noinspection ConstantConditions
                    addField(field, choiceValue, choiceDataType);
                    break;

                case MAP:

                    DataType mapValueType = ((MapDataType) dataType).getValueType();
                    if (mapValueType == null) {
                        mapValueType = defaultValueType;
                    }

                    Map<String, Object> map = DataTypeUtils.toMap(value, field);

                    //noinspection ConstantConditions
                    storeMap(map, mapValueType, this::addField);

                    break;

                case ARRAY:
                case RECORD:
                    handleComplexField(value, dataType, field, this::addField);
                    break;

                default:
                    unsupportedType(dataType, field);
            }
        }
    }

    private void nullBehaviour(@NonNull final String fieldName) {

        Objects.requireNonNull(fieldName, "Field name is required");

        if (WriteOptions.NullValueBehaviour.FAIL.equals(options.getNullValueBehaviour())) {
            String message = String.format(PutInfluxDBRecord.FIELD_NULL_VALUE, fieldName);

            throw new IllegalStateException(message);
        }
    }
}
