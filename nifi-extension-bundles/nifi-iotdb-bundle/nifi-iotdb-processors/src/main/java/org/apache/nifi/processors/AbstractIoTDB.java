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
package org.apache.nifi.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.model.DatabaseField;
import org.apache.nifi.processors.model.DatabaseSchema;
import org.apache.nifi.processors.model.ValidationResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractIoTDB extends AbstractProcessor {
    private static final int DEFAULT_IOTDB_PORT = 6667;

    protected static ObjectMapper mapper = new ObjectMapper();

    private static final String FIELDS = "fields";

    private static final Map<RecordFieldType, TSDataType> typeMap =
            new HashMap<>();

    private static final Map<String, RecordFieldType> reversedTypeMap =
            new HashMap<>();

    static final Set<RecordFieldType> supportedType =
            new HashSet<>();

    static final PropertyDescriptor IOTDB_HOST = new PropertyDescriptor.Builder()
            .name("Host")
            .description("IoTDB server host address")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor IOTDB_PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("IoTDB server port number")
            .defaultValue(String.valueOf(DEFAULT_IOTDB_PORT))
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username for access to IoTDB")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password for access to IoTDB")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Processing succeeded")
            .build();

    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Processing failed")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        IOTDB_HOST,
        IOTDB_PORT,
        USERNAME,
        PASSWORD
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS,
        REL_FAILURE
    );

    static {
        typeMap.put(RecordFieldType.STRING, TSDataType.TEXT);
        typeMap.put(RecordFieldType.BOOLEAN, TSDataType.BOOLEAN);
        typeMap.put(RecordFieldType.INT, TSDataType.INT32);
        typeMap.put(RecordFieldType.LONG, TSDataType.INT64);
        typeMap.put(RecordFieldType.FLOAT, TSDataType.FLOAT);
        typeMap.put(RecordFieldType.DOUBLE, TSDataType.DOUBLE);
        for (final Map.Entry<RecordFieldType, TSDataType> it : typeMap.entrySet()) {
            reversedTypeMap.put(String.valueOf(it.getValue()), it.getKey());
        }

        supportedType.add(RecordFieldType.BOOLEAN);
        supportedType.add(RecordFieldType.STRING);
        supportedType.add(RecordFieldType.INT);
        supportedType.add(RecordFieldType.LONG);
        supportedType.add(RecordFieldType.FLOAT);
        supportedType.add(RecordFieldType.DOUBLE);
        supportedType.add(RecordFieldType.TIMESTAMP);
        supportedType.add(RecordFieldType.TIME);
        supportedType.add(RecordFieldType.DATE);
    }

    protected final AtomicReference<Session> session = new AtomicReference<>(null);
    private volatile String transitUri;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IoTDBConnectionException {
        if (session.get() == null) {
            final String host = context.getProperty(IOTDB_HOST).getValue();
            final int port = Integer.parseInt(context.getProperty(IOTDB_PORT).getValue());
            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).getValue();

            transitUri = "iotdb://%s:%d".formatted(host, port);
            session.set(new Session.Builder()
                    .host(host)
                    .port(port)
                    .username(username)
                    .password(password)
                    .build());
            session.get().open();
        }
    }

    @OnStopped
    public void stop() {
        if (session.get() != null) {
            try {
                session.get().close();
            } catch (final IoTDBConnectionException e) {
                getLogger().error("IoTDB disconnection failed", e);
            }
            session.set(null);
        }
        transitUri = null;
    }

    protected String getTransitUri() {
        return transitUri;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    protected TSDataType getType(final RecordFieldType type) {
        return typeMap.get(type);
    }

    protected RecordFieldType getType(final String type) {
        return reversedTypeMap.get(type);
    }

    protected ValidationResult validateSchemaAttribute(final String schemaAttribute) {
        final JsonNode schema;
        try {
            schema = mapper.readTree(schemaAttribute);
        } catch (final JsonProcessingException e) {
            return new ValidationResult(false, e.getMessage());
        }
        final Set<String> keySet = new HashSet<>();
        schema.fieldNames().forEachRemaining(keySet::add);

        if (!keySet.contains(FIELDS)) {
            final String msg = "The JSON of schema must contain `fields`";
            return new ValidationResult(false, msg);
        }

        for (int i = 0; i < schema.get(FIELDS).size(); i++) {
            final JsonNode field = schema.get(FIELDS).get(i);
            final Set<String> fieldKeySet = new HashSet<>();

            field.fieldNames().forEachRemaining(fieldKeySet::add);
            if (!fieldKeySet.contains("tsName") || !fieldKeySet.contains("dataType")) {
                final String msg = "`tsName` or `dataType` has not been set";
                return new ValidationResult(false, msg);
            }

            if (!DatabaseField.getSupportedDataType().contains(field.get("dataType").asText())) {
                final String msg =
                        String.format(
                                "Unknown `dataType`: %s. The supported dataTypes are %s",
                                field.get("dataType").asText(), DatabaseField.getSupportedDataType());
                return new ValidationResult(false, msg);
            }

            final Set<String> supportedKeySet = new HashSet<>();
            supportedKeySet.add("tsName");
            supportedKeySet.add("dataType");
            supportedKeySet.add("encoding");
            supportedKeySet.add("compressionType");

            final Set<String> tmpKetSet = new HashSet<>();
            tmpKetSet.addAll(supportedKeySet);
            tmpKetSet.addAll(fieldKeySet);
            tmpKetSet.removeAll(supportedKeySet);
            if (!tmpKetSet.isEmpty()) {
                final String msg = "Unknown property or properties: " + tmpKetSet;
                return new ValidationResult(false, msg);
            }

            if (fieldKeySet.contains("compressionType") && !fieldKeySet.contains("encoding")) {
                final String msg =
                        "The `compressionType` has been set, but the `encoding` has not. The property `compressionType` will not take effect";
                return new ValidationResult(true, msg);
            }

            if (field.get("encoding") != null
                    && !DatabaseField.getSupportedEncoding().contains(field.get("encoding").asText())) {
                final String msg =
                        String.format(
                                "Unknown `encoding`: %s, The supported encoding types are %s",
                                field.get("encoding").asText(), DatabaseField.getSupportedEncoding());
                return new ValidationResult(false, msg);
            }

            if (field.get("compressionType") != null
                    && !DatabaseField.getSupportedCompressionType().contains(field.get("compressionType").asText())) {
                final String msg =
                        String.format(
                                "Unknown `compressionType`: %s, The supported compressionType are %s",
                                field.get("compressionType").asText(), DatabaseField.getSupportedCompressionType());
                return new ValidationResult(false, msg);
            }
        }

        return new ValidationResult(true, null);
    }

    protected ValidationResult validateSchema(final String timeField, final RecordSchema recordSchema) {
        final List<String> fieldNames = recordSchema.getFieldNames();
        final List<DataType> dataTypes = recordSchema.getDataTypes();
        if (!fieldNames.contains(timeField)) {
            return new ValidationResult(false, "The fields must contain " + timeField);
        }
        fieldNames.remove(timeField);
        for (final DataType type : dataTypes) {
            final RecordFieldType dataType = type.getFieldType();
            if (!supportedType.contains(dataType)) {
                final String msg =
                        String.format(
                                "Unknown `dataType`: %s. The supported dataTypes are %s",
                                dataType.toString(), supportedType);
                return new ValidationResult(false, msg);
            }
        }

        return new ValidationResult(true, null);
    }

    protected Map<String, List<String>> parseSchema(final List<String> fieldNames) {
        final Map<String, List<String>> deviceMeasurementMap = new LinkedHashMap<>();
        fieldNames.forEach(
                field -> {
                    final List<String> paths = new ArrayList<>(Arrays.asList(field.split("\\.")));
                    final int lastIndex = paths.size() - 1;
                    final String lastPath = paths.remove(lastIndex);
                    final String device = String.join(".", paths);
                    if (!deviceMeasurementMap.containsKey(device)) {
                        deviceMeasurementMap.put(device, new ArrayList<>());
                    }
                    deviceMeasurementMap.get(device).add(lastPath);
                });

        return deviceMeasurementMap;
    }

    protected Map<String, Tablet> generateTablets(final DatabaseSchema schema, final String prefix, final int maxRowNumber) {
        final Map<String, List<String>> deviceMeasurementMap = parseSchema(schema.getFieldNames(prefix));
        final Map<String, Tablet> tablets = new LinkedHashMap<>();
        deviceMeasurementMap.forEach(
                (device, measurements) -> {
                    final List<IMeasurementSchema> schemas = new ArrayList<>();
                    for (final String measurement : measurements) {
                        final TSDataType dataType = schema.getDataType(measurement);
                        final TSEncoding encoding = schema.getEncodingType(measurement);
                        final CompressionType compressionType = schema.getCompressionType(measurement);
                        if (encoding == null) {
                            schemas.add(new MeasurementSchema(measurement, dataType));
                        } else if (compressionType == null) {
                            schemas.add(new MeasurementSchema(measurement, dataType, encoding));
                        } else {
                            schemas.add(new MeasurementSchema(measurement, dataType, encoding, compressionType));
                        }
                    }
                    final Tablet tablet = new Tablet(device, schemas, maxRowNumber);
                    tablets.put(device, tablet);
                });
        return tablets;
    }

    protected Object convertType(final Object value, final TSDataType type) {
        return switch (type) {
            case TEXT -> new Binary(String.valueOf(value), StandardCharsets.UTF_8);
            case INT32 -> Integer.parseInt(value.toString());
            case INT64 -> Long.parseLong(value.toString());
            case FLOAT -> Float.parseFloat(value.toString());
            case DOUBLE -> Double.parseDouble(value.toString());
            case BOOLEAN -> Boolean.parseBoolean(value.toString());
            default -> null;
        };
    }

    protected DatabaseSchema convertSchema(final String timeField, final RecordSchema recordSchema) {
        final List<String> fieldNames = recordSchema.getFieldNames();
        fieldNames.remove(timeField);

        final List<DatabaseField> fields = new ArrayList<>();
        fieldNames.forEach(fieldName -> {
            final Optional<DataType> dataTypeFound = recordSchema.getDataType(fieldName);
            final DataType dataType = dataTypeFound.orElseThrow(() -> new IllegalArgumentException(String.format("Field [%s] Data Type not found", fieldName)));
            final RecordFieldType recordFieldType = dataType.getFieldType();
            final TSDataType timeSeriesDataType = getType(recordFieldType);
            final DatabaseField field = new DatabaseField(fieldName, timeSeriesDataType);
            fields.add(field);
        });
        return new DatabaseSchema(fields);
    }
}
