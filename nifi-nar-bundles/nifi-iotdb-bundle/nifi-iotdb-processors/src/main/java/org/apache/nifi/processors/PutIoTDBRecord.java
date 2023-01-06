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

import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.processors.model.IoTDBSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.model.ValidationResult;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.Date;

@Tags({"iotdb", "insert", "tablet"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "This is a record aware processor that reads the content of the incoming FlowFile as individual records using the "
                + "configured 'Record Reader' and writes them to Apache IoTDB using native interface.")
public class PutIoTDBRecord extends AbstractIoTDB {

    static final PropertyDescriptor RECORD_READER_FACTORY =
            new PropertyDescriptor.Builder()
                    .name("Record Reader")
                    .description(
                            "Specifies the type of Record Reader controller service to use for parsing the incoming data "
                                    + "and determining the schema")
                    .identifiesControllerService(RecordReaderFactory.class)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(true)
                    .build();

    static final PropertyDescriptor TIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("Time Field")
                    .description(
                            "The name of time field.")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("Time")
                    .required(true)
                    .sensitive(true)
                    .build();

    static final PropertyDescriptor SCHEMA =
            new PropertyDescriptor.Builder()
                    .name("Schema Template")
                    .description(
                            "The Apache IoTDB Schema Template defined using JSON. " +
                                    "The Processor will infer the IoTDB Schema when this property is not configured. " +
                                    "Besides, you can set encoding type and compression type by this method. " +
                                    "If you want to know more detail about this, you can browse this link: https://iotdb.apache.org/UserGuide/Master/Ecosystem-Integration/NiFi-IoTDB.html")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(false)
                    .build();

    static final PropertyDescriptor PREFIX =
            new PropertyDescriptor.Builder()
                    .name("Prefix")
                    .description("The timeseries prefix where records will be stored. The prefix must begin with 'root' and end with '.'")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(true)
                    .build();

    static final PropertyDescriptor ALIGNED =
            new PropertyDescriptor.Builder()
                    .name("Aligned")
                    .description("Whether to use the Apache IoTDB Aligned Timeseries interface")
                    .allowableValues("true", "false")
                    .defaultValue("false")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(false)
                    .defaultValue("false")
                    .build();

    static final PropertyDescriptor MAX_ROW_NUMBER =
            new PropertyDescriptor.Builder()
                    .name("Max Row Number")
                    .description(
                            "Specifies the max row number of each Apache IoTDB Tablet")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(false)
                    .defaultValue("1024")
                    .build();

    private static final String ROOTPREFIX = "root.";

    {
        descriptors.add(RECORD_READER_FACTORY);
        descriptors.add(TIME_FIELD);
        descriptors.add(PREFIX);
        descriptors.add(SCHEMA);
        descriptors.add(ALIGNED);
        descriptors.add(MAX_ROW_NUMBER);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession)
            throws ProcessException {

        FlowFile flowFile = processSession.get();

        if (flowFile == null) {
            return;
        }

        String schemaProperty = processContext.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        String alignedProperty = processContext.getProperty(ALIGNED).evaluateAttributeExpressions(flowFile).getValue();
        String maxRowNumberProperty = processContext.getProperty(MAX_ROW_NUMBER).evaluateAttributeExpressions(flowFile).getValue();
        String prefix = processContext.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        String timeField = processContext.getProperty(TIME_FIELD).evaluateAttributeExpressions(flowFile).getValue();
        final RecordReaderFactory recordParserFactory =
                processContext
                        .getProperty(RECORD_READER_FACTORY)
                        .asControllerService(RecordReaderFactory.class);

        if (!prefix.startsWith(ROOTPREFIX) || !prefix.endsWith(".")) {
            getLogger().error("The prefix is not begin with root and end with .", flowFile);
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        final boolean aligned = Boolean.valueOf(alignedProperty);
        int maxRowNumber = Integer.valueOf(maxRowNumberProperty);

        try (final InputStream inputStream = processSession.read(flowFile);
             final RecordReader recordReader =
                     recordParserFactory.createRecordReader(flowFile, inputStream, getLogger())) {
            IoTDBSchema schema = getSchema(timeField, schemaProperty, recordReader);

            //List<String> fieldNames = schema.getFieldNames(prefix);

            HashMap<String, Tablet> tablets = generateTablets(schema, prefix, maxRowNumber);
            DateTimeFormatter format = null;

            Record record;

            while ((record = recordReader.nextRecord()) != null) {
                long timestamp = getTimestamp(timeField, record);
                boolean isFulled = false;

                for (Map.Entry<String, Tablet> entry : tablets.entrySet()) {
                    String device = entry.getKey();
                    Tablet tablet = entry.getValue();
                    int rowIndex = tablet.rowSize++;

                    tablet.addTimestamp(rowIndex, timestamp);
                    List<MeasurementSchema> measurements = tablet.getSchemas();
                    for (MeasurementSchema measurement : measurements) {
                        String id = measurement.getMeasurementId();
                        TSDataType type = measurement.getType();
                        Object value = getValue(record.getValue(id), type);
                        tablet.addValue(id, rowIndex, value);
                    }
                    isFulled = tablet.rowSize == tablet.getMaxRowNumber();
                }
                if (isFulled) {
                    if (aligned) {
                        session.get().insertAlignedTablets(tablets);
                    } else {
                        session.get().insertTablets(tablets);
                    }
                    tablets.values().forEach(tablet -> tablet.reset());
                }
            }

            AtomicBoolean hasRest = new AtomicBoolean(false);
            tablets.forEach(
                    (device, tablet) -> {
                        if (hasRest.get() == false && tablet.rowSize != 0) {
                            hasRest.set(true);
                        }
                    });
            if (hasRest.get()) {
                if (aligned) {
                    session.get().insertAlignedTablets(tablets);
                } else {
                    session.get().insertTablets(tablets);
                }
            }
        } catch (Exception e) {
            getLogger().error("Processing failed {}", flowFile, e);
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }
        processSession.transfer(flowFile, REL_SUCCESS);
    }

    private IoTDBSchema getSchema(String timeField, String property, RecordReader recordReader) throws Exception {
        ValidationResult result;
        IoTDBSchema schema;
        result =
                property != null
                        ? validateSchemaAttribute(property)
                        : validateSchema(timeField, recordReader.getSchema());

        if (!result.getKey()) {
            getLogger().error("The property `schema` has an error: {}", result.getValue());
            throw new Exception();

        } else {
            if (result.getValue() != null) {
                getLogger().warn("The property `schema` has a warn: {}", result.getValue());
            }
        }

        schema =
                property != null
                        ? mapper.readValue(property, IoTDBSchema.class)
                        : convertSchema(timeField, recordReader.getSchema());
        return schema;
    }

    private long getTimestamp(String timeField, Record record) {
        long timestamp;
        Object time = record.getValue(timeField);
        if (time instanceof  Timestamp) {
            Timestamp temp = (Timestamp) time;
            timestamp = temp.getTime();
        } else if (time instanceof  Time) {
            Time temp = (Time) time;
            timestamp = temp.getTime();
        } else if (time instanceof  Date) {
            Date temp = (Date) time;
            timestamp = temp.getTime();
        } else if (time instanceof Long) {
            timestamp = (Long) time;
        } else {
            throw new IllegalArgumentException(String.format("Unexpected Time Field: %s", time));
        }
        return timestamp;
    }

    private Object getValue(Object value, TSDataType type) throws Exception {
        if (value != null) {
            try {
                value = convertType(value, type);
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("The value [%s] cannot be converted to the type [%s]",
                        value, type));
            }
        } else {
            value = null;
        }
        return value;
    }
}
