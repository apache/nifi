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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processors.model.Field;
import org.apache.nifi.processors.model.IoTDBSchema;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.model.ValidationResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public abstract class AbstractIoTDB extends AbstractProcessor {
    private static final int DEFAULT_IOTDB_PORT = 6667;

    protected static ObjectMapper mapper = new ObjectMapper();

    private static final String TIME_TYPE = "timeType";
    private static final String FIELDS = "fields";
    private static final String TIME = "Time";
    private static final String PREFIX = "root.";

    private static Map<RecordFieldType, TSDataType> typeMap =
            new HashMap<>();

    static final Set<RecordFieldType> supportedType =
            new HashSet<>();

    static final Set<RecordFieldType> supportedTimeType =
            new HashSet<>();

    protected static final String[] STRING_TIME_FORMAT =
            new String[]{
                    "yyyy-MM-dd HH:mm:ss.SSSX",
                    "yyyy/MM/dd HH:mm:ss.SSSX",
                    "yyyy.MM.dd HH:mm:ss.SSSX",
                    "yyyy-MM-dd HH:mm:ssX",
                    "yyyy/MM/dd HH:mm:ssX",
                    "yyyy.MM.dd HH:mm:ssX",
                    "yyyy-MM-dd HH:mm:ss.SSSz",
                    "yyyy/MM/dd HH:mm:ss.SSSz",
                    "yyyy.MM.dd HH:mm:ss.SSSz",
                    "yyyy-MM-dd HH:mm:ssz",
                    "yyyy/MM/dd HH:mm:ssz",
                    "yyyy.MM.dd HH:mm:ssz",
                    "yyyy-MM-dd HH:mm:ss.SSS",
                    "yyyy/MM/dd HH:mm:ss.SSS",
                    "yyyy.MM.dd HH:mm:ss.SSS",
                    "yyyy-MM-dd HH:mm:ss",
                    "yyyy/MM/dd HH:mm:ss",
                    "yyyy.MM.dd HH:mm:ss",
                    "yyyy-MM-dd'T'HH:mm:ss.SSSX",
                    "yyyy/MM/dd'T'HH:mm:ss.SSSX",
                    "yyyy.MM.dd'T'HH:mm:ss.SSSX",
                    "yyyy-MM-dd'T'HH:mm:ssX",
                    "yyyy/MM/dd'T'HH:mm:ssX",
                    "yyyy.MM.dd'T'HH:mm:ssX",
                    "yyyy-MM-dd'T'HH:mm:ss.SSSz",
                    "yyyy/MM/dd'T'HH:mm:ss.SSSz",
                    "yyyy.MM.dd'T'HH:mm:ss.SSSz",
                    "yyyy-MM-dd'T'HH:mm:ssz",
                    "yyyy/MM/dd'T'HH:mm:ssz",
                    "yyyy.MM.dd'T'HH:mm:ssz",
                    "yyyy-MM-dd'T'HH:mm:ss.SSS",
                    "yyyy/MM/dd'T'HH:mm:ss.SSS",
                    "yyyy.MM.dd'T'HH:mm:ss.SSS",
                    "yyyy-MM-dd'T'HH:mm:ss",
                    "yyyy/MM/dd'T'HH:mm:ss",
                    "yyyy.MM.dd'T'HH:mm:ss"
            };

    static final PropertyDescriptor IOTDB_HOST =
            new PropertyDescriptor.Builder()
                    .name("Host")
                    .description("The host of IoTDB.")
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(true)
                    .build();

    static final PropertyDescriptor IOTDB_PORT =
            new PropertyDescriptor.Builder()
                    .name("Port")
                    .description("The port of IoTDB.")
                    .defaultValue(String.valueOf(DEFAULT_IOTDB_PORT))
                    .addValidator(StandardValidators.PORT_VALIDATOR)
                    .required(true)
                    .build();

    static final PropertyDescriptor USERNAME =
            new PropertyDescriptor.Builder()
                    .name("Username")
                    .description("Username to access the IoTDB.")
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(true)
                    .build();

    static final PropertyDescriptor PASSWORD =
            new PropertyDescriptor.Builder()
                    .name("Password")
                    .description("Password to access the IoTDB.")
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(true)
                    .sensitive(true)
                    .build();

    protected static List<PropertyDescriptor> descriptors = new ArrayList<>();

    protected static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("files that were successfully processed")
                    .build();
    protected static final Relationship REL_FAILURE =
            new Relationship.Builder()
                    .name("failure")
                    .description("files that were not successfully processed")
                    .build();

    protected static Set<Relationship> relationships = new HashSet<>();

    static {
        descriptors.add(IOTDB_HOST);
        descriptors.add(IOTDB_PORT);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);

        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);

        typeMap.put(RecordFieldType.STRING, TSDataType.TEXT);
        typeMap.put(RecordFieldType.BOOLEAN, TSDataType.BOOLEAN);
        typeMap.put(RecordFieldType.INT, TSDataType.INT32);
        typeMap.put(RecordFieldType.LONG, TSDataType.INT64);
        typeMap.put(RecordFieldType.FLOAT, TSDataType.FLOAT);
        typeMap.put(RecordFieldType.DOUBLE, TSDataType.DOUBLE);

        supportedType.add(RecordFieldType.BOOLEAN);
        supportedType.add(RecordFieldType.STRING);
        supportedType.add(RecordFieldType.INT);
        supportedType.add(RecordFieldType.LONG);
        supportedType.add(RecordFieldType.FLOAT);
        supportedType.add(RecordFieldType.DOUBLE);

        supportedTimeType.add(RecordFieldType.STRING);
        supportedTimeType.add(RecordFieldType.LONG);
    }

    protected final AtomicReference<Session> session = new AtomicReference<>(null);

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IoTDBConnectionException {
        connectToIoTDB(context);
    }

    void connectToIoTDB(ProcessContext context) throws IoTDBConnectionException {
        if (session.get() == null) {
            final String host = context.getProperty(IOTDB_HOST).getValue();
            final int port = Integer.parseInt(context.getProperty(IOTDB_PORT).getValue());
            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).getValue();

            session.set(
                    new Session.Builder()
                            .host(host)
                            .port(port)
                            .username(username)
                            .password(password)
                            .build());
            session.get().open();
        }
    }

    public void stop(ProcessContext context) {
        if (session.get() != null) {
            try {
                session.get().close();
            } catch (IoTDBConnectionException e) {
                getLogger().error("IoTDB disconnection failed", e);
            }
            session.set(null);
        }
    }

    protected TSDataType getType(RecordFieldType type) {
        return typeMap.get(type);
    }

    protected ValidationResult validateSchemaAttribute(String schemaAttribute) {
        JsonNode schema = null;
        try {
            schema = mapper.readTree(schemaAttribute);
        } catch (JsonProcessingException e) {
            return new ValidationResult(false, e.getMessage());
        }
        Set<String> keySet = new HashSet<>();
        schema.fieldNames().forEachRemaining(field -> keySet.add(field));

        if (!keySet.contains(TIME_TYPE) || !keySet.contains(FIELDS)) {
            String msg = "The JSON of schema must contain `timeType` and `fields`";
            return new ValidationResult(false, msg);
        }

        if (!IoTDBSchema.getSupportedTimeType().contains(schema.get(TIME_TYPE).asText())) {
            String msg =
                    String.format(
                            "Unknown `timeType`: %s, there are only two options `LONG` and `STRING` for this property",
                            schema.get(TIME_TYPE).asText());
            return new ValidationResult(false, msg);
        }

        for (int i = 0; i < schema.get(FIELDS).size(); i++) {
            JsonNode field = schema.get(FIELDS).get(i);
            Set<String> fieldKeySet = new HashSet<>();

            field.fieldNames().forEachRemaining(fieldName -> fieldKeySet.add(fieldName));
            if (!fieldKeySet.contains("tsName") || !fieldKeySet.contains("dataType")) {
                String msg = "`tsName` or `dataType` has not been set";
                return new ValidationResult(false, msg);
            }

            if (!field.get("tsName").asText().startsWith(PREFIX)) {
                String msg =
                        String.format("The tsName `%s` is not start with 'root.'", field.get("tsName").asText());
                return new ValidationResult(false, msg);
            }

            if (!Field.getSupportedDataType().contains(field.get("dataType").asText())) {
                String msg =
                        String.format(
                                "Unknown `dataType`: %s. The supported dataTypes are %s",
                                field.get("dataType").asText(), Field.getSupportedDataType());
                return new ValidationResult(false, msg);
            }

            Set<String> supportedKeySet = new HashSet<>();
            supportedKeySet.add("tsName");
            supportedKeySet.add("dataType");
            supportedKeySet.add("encoding");
            supportedKeySet.add("compressionType");

            HashSet<String> tmpKetSet = new HashSet<>();
            tmpKetSet.addAll(supportedKeySet);
            tmpKetSet.addAll(fieldKeySet);
            tmpKetSet.removeAll(supportedKeySet);
            if (!tmpKetSet.isEmpty()) {
                String msg = "Unknown property or properties: " + tmpKetSet;
                return new ValidationResult(false, msg);
            }

            if (fieldKeySet.contains("compressionType") && !fieldKeySet.contains("encoding")) {
                String msg =
                        "The `compressionType` has been set, but the `encoding` has not. The property `compressionType` will not take effect";
                return new ValidationResult(true, msg);
            }

            if (field.get("encoding") != null
                    && !Field.getSupportedEncoding().contains(field.get("encoding").asText())) {
                String msg =
                        String.format(
                                "Unknown `encoding`: %s, The supported encoding types are %s",
                                field.get("encoding").asText(), Field.getSupportedEncoding());
                return new ValidationResult(false, msg);
            }

            if (field.get("compressionType") != null
                    && !Field.getSupportedCompressionType().contains(field.get("compressionType"))) {
                String msg =
                        String.format(
                                "Unknown `compressionType`: %s, The supported compressionType are %s",
                                field.get("compressionType").asText(), Field.getSupportedCompressionType());
                return new ValidationResult(false, msg);
            }
        }

        return new ValidationResult(true, null);
    }

    protected ValidationResult validateSchema(RecordSchema recordSchema) {
        List<String> fieldNames = recordSchema.getFieldNames();
        List<DataType> dataTypes = recordSchema.getDataTypes();
        if (!fieldNames.contains(TIME)) {
            return new ValidationResult(false, "The fields must contain `Time`");
        }
        if (!supportedTimeType.contains(recordSchema.getDataType(TIME).get().getFieldType())) {
            return new ValidationResult(false, "The dataType of `Time` must be `STRING` or `LONG`");
        }
        fieldNames.remove(TIME);
        for (String fieldName : fieldNames) {
            if (!fieldName.startsWith(PREFIX)) {
                String msg = String.format("The tsName `%s` is not start with 'root.'", fieldName);
                return new ValidationResult(false, msg);
            }
        }
        for (DataType type : dataTypes) {
            RecordFieldType dataType = type.getFieldType();
            if (!supportedType.contains(dataType)) {
                String msg =
                        String.format(
                                "Unknown `dataType`: %s. The supported dataTypes are %s",
                                dataType.toString(), supportedType);
                return new ValidationResult(false, msg);
            }
        }

        return new ValidationResult(true, null);
    }

    protected Map<String, List<String>> parseSchema(List<String> filedNames) {
        HashMap<String, List<String>> deviceMeasurementMap = new HashMap<>();
        filedNames.stream()
                .forEach(
                        filed -> {
                            String[] paths = filed.split("\\.");
                            String device = StringUtils.join(paths, ".", 0, paths.length - 1);

                            if (!deviceMeasurementMap.containsKey(device)) {
                                deviceMeasurementMap.put(device, new ArrayList<>());
                            }
                            deviceMeasurementMap.get(device).add(paths[paths.length - 1]);
                        });

        return deviceMeasurementMap;
    }

    protected HashMap<String, Tablet> generateTablets(IoTDBSchema schema, int maxRowNumber) {
        Map<String, List<String>> deviceMeasurementMap = parseSchema(schema.getFieldNames());
        HashMap<String, Tablet> tablets = new HashMap<>();
        deviceMeasurementMap.forEach(
                (device, measurements) -> {
                    ArrayList<MeasurementSchema> schemas = new ArrayList<>();
                    for (String measurement : measurements) {
                        String tsName = device + "." + measurement;
                        TSDataType dataType = schema.getDataType(tsName);
                        TSEncoding encoding = schema.getEncodingType(tsName);
                        CompressionType compressionType = schema.getCompressionType(tsName);
                        if (encoding == null) {
                            schemas.add(new MeasurementSchema(measurement, dataType));
                        } else if (compressionType == null) {
                            schemas.add(new MeasurementSchema(measurement, dataType, encoding));
                        } else {
                            schemas.add(new MeasurementSchema(measurement, dataType, encoding, compressionType));
                        }
                    }
                    Tablet tablet = new Tablet(device, schemas, maxRowNumber);
                    tablets.put(device, tablet);
                });
        return tablets;
    }

    protected String initFormatter(String time) {
        for (String format : STRING_TIME_FORMAT) {
            try {
                DateTimeFormatter.ofPattern(format).parse(time);
                return format;
            } catch (DateTimeParseException e) {

            }
        }
        return null;
    }

    protected Object convertType(Object value, TSDataType type) {
        switch (type) {
            case TEXT:
                return Binary.valueOf(String.valueOf(value));
            case INT32:
                return Integer.parseInt(value.toString());
            case INT64:
                return Long.parseLong(value.toString());
            case FLOAT:
                return Float.parseFloat(value.toString());
            case DOUBLE:
                return Double.parseDouble(value.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(value.toString());
            default:
                return null;
        }
    }

    protected IoTDBSchema convertSchema(RecordSchema recordSchema) {
        String timeType =
                recordSchema.getDataType(TIME).get().getFieldType() == RecordFieldType.LONG
                        ? "LONG"
                        : "STRING";
        List<String> fieldNames = recordSchema.getFieldNames();
        fieldNames.remove(TIME);

        ArrayList<Field> fields = new ArrayList<>();
        fieldNames.forEach(
                fieldName ->
                        fields.add(
                                new Field(
                                        fieldName, getType(recordSchema.getDataType(fieldName).get().getFieldType()))));
        IoTDBSchema schema = new IoTDBSchema(timeType, fields);
        return schema;
    }
}
