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
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
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

@Tags({"iotdb", "insert", "tablet"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "This is a record aware processor that reads the content of the incoming FlowFile as individual records using the "
                + "configured 'Record Reader' and writes them to Apache IoTDB using native interface.")
public class PutIoTDB extends AbstractIoTDB {

    static final PropertyDescriptor RECORD_READER_FACTORY =
            new PropertyDescriptor.Builder()
                    .name("Record Reader")
                    .description(
                            "Specifies the type of Record Reader controller service to use for parsing the incoming data "
                                    + "and determining the schema")
                    .identifiesControllerService(RecordReaderFactory.class)
                    .required(true)
                    .build();

    static final PropertyDescriptor TIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("Time Field")
                    .description(
                            "The field name which represents time")
                    .defaultValue("Time")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(false)
                    .build();

    static final PropertyDescriptor SCHEMA =
            new PropertyDescriptor.Builder()
                    .name("Schema")
                    .description(
                            "The schema that IoTDB needs doesn't support good by NiFi.\n"
                                    + "Therefore, you can define the schema here.\n"
                                    + "Besides, you can set encoding type and compression type by this method.\n"
                                    + "If you don't set this property, the inferred schema will be used.\n")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .required(false)
                    .build();

    static final PropertyDescriptor ALIGNED =
            new PropertyDescriptor.Builder()
                    .name("Aligned")
                    .description("Whether to use the Apache IoTDB Aligned Timeseries interface")
                    .allowableValues("true", "false")
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .defaultValue("false")
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .required(false)
                    .build();

    static final PropertyDescriptor MAX_ROW_NUMBER =
            new PropertyDescriptor.Builder()
                    .name("Max Row Number")
                    .description(
                            "Specifies the max row number of each Apache IoTDB Tablet")
                    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .required(false)
                    .build();

    static {
        descriptors.add(RECORD_READER_FACTORY);
        descriptors.add(TIME_FIELD);
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
        final RecordReaderFactory recordParserFactory =
                processContext
                        .getProperty(RECORD_READER_FACTORY)
                        .asControllerService(RecordReaderFactory.class);

        FlowFile flowFile = processSession.get();

        if (flowFile == null) {
            processSession.transfer(flowFile, REL_SUCCESS);
            return;
        }

        String timeFieldProperty = processContext.getProperty(TIME_FIELD).evaluateAttributeExpressions(flowFile).getValue();
        String schemaProperty = processContext.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        String alignedProperty = processContext.getProperty(ALIGNED).evaluateAttributeExpressions(flowFile).getValue();
        String maxRowNumberProperty = processContext.getProperty(MAX_ROW_NUMBER).evaluateAttributeExpressions(flowFile).getValue();

        String timeField = timeFieldProperty != null ? timeFieldProperty : "Time";
        final boolean aligned = alignedProperty != null ? Boolean.valueOf(alignedProperty) : false;
        int maxRowNumber = maxRowNumberProperty != null ? Integer.valueOf(maxRowNumberProperty) : 1024;

        try (final InputStream inputStream = processSession.read(flowFile);
             final RecordReader recordReader =
                     recordParserFactory.createRecordReader(flowFile, inputStream, getLogger())) {
            boolean needInitFormatter;
            IoTDBSchema schema;
            ValidationResult result;

            result =
                    schemaProperty != null
                            ? validateSchemaAttribute(schemaProperty)
                            : validateSchema(recordReader.getSchema());

            if (!result.getKey()) {
                getLogger().error(String.format("The property `schema` has an error: %s", result.getValue()));
                inputStream.close();
                recordReader.close();
                processSession.transfer(flowFile, REL_FAILURE);
                return;
            } else {
                if (result.getValue() != null) {
                    getLogger().warn(String.format("The property `schema` has a warn: %s", result.getValue()));
                }
            }

            schema =
                    schemaProperty != null
                            ? mapper.readValue(schemaProperty, IoTDBSchema.class)
                            : convertSchema(recordReader.getSchema());

            List<String> fieldNames = schema.getFieldNames();

            needInitFormatter = schema.getTimeType() != IoTDBSchema.TimeType.LONG;

            HashMap<String, Tablet> tablets = generateTablets(schema, maxRowNumber);
            String format = null;

            Record record;

            while ((record = recordReader.nextRecord()) != null) {
                Object[] values = record.getValues();
                if (format == null && needInitFormatter) {
                    format = initFormatter((String) values[0]);
                    if (format == null) {
                        getLogger().error("{} Record [{}] time format not supported\", flowFile, recordNumber");
                        inputStream.close();
                        recordReader.close();
                        processSession.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                }

                long timestamp;
                if (needInitFormatter) {
                    timestamp = record.getAsDate(timeField, format).getTime();
                } else {
                    timestamp = (Long) values[0];
                }

                boolean isFulled = false;

                for (Map.Entry<String, Tablet> entry : tablets.entrySet()) {
                    String device = entry.getKey();
                    Tablet tablet = entry.getValue();
                    int rowIndex = tablet.rowSize++;

                    tablet.addTimestamp(rowIndex, timestamp);
                    List<MeasurementSchema> measurements = tablet.getSchemas();
                    for (MeasurementSchema measurement : measurements) {
                        String tsName =
                                new StringBuilder()
                                        .append(device)
                                        .append(".")
                                        .append(measurement.getMeasurementId())
                                        .toString();
                        int valueIndex = fieldNames.indexOf(tsName) + 1;
                        Object value;
                        TSDataType type = measurement.getType();
                        try {
                            value = convertType(values[valueIndex], type);
                        } catch (Exception e) {
                            getLogger().warn(String.format("The value `%s` can't be converted to the type `%s`", values[valueIndex], type));
                            value = null;
                        }
                        tablet.addValue(measurement.getMeasurementId(), rowIndex, value);
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
            inputStream.close();
            recordReader.close();
            processSession.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Processing failed {}", flowFile, e);
            processSession.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    public boolean isStateful(ProcessContext context) {
        return super.isStateful(context);
    }

    @OnUnscheduled
    public void stop(ProcessContext context) {
        super.stop(context);
    }
}
