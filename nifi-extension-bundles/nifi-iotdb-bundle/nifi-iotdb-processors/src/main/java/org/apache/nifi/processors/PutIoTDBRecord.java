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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.processors.model.DatabaseSchema;
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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.model.ValidationResult;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.Date;

@Tags({"IoT", "Timeseries"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Read input FlowFile Records and write to Apache IoTDB")
public class PutIoTDBRecord extends AbstractIoTDB {

    static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("Prefix")
            .description("The Timeseries prefix where records will be stored. The prefix must begin with [root] and end with [.]")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor TIME_FIELD = new PropertyDescriptor.Builder()
            .name("Time Field")
            .description("The name of field containing the timestamp in FlowFile Records")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Time")
            .required(true)
            .build();

    static final PropertyDescriptor ALIGNED = new PropertyDescriptor.Builder()
            .name("Aligned")
            .description("Whether to use the Apache IoTDB Aligned Timeseries interface")
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .defaultValue(Boolean.FALSE.toString())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor MAX_ROW_NUMBER = new PropertyDescriptor.Builder()
            .name("Max Row Number")
            .description("Maximum row number of each Apache IoTDB Tablet")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .defaultValue("1024")
            .build();

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Record Reader used for parsing the incoming FlowFiles and determining the schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor SCHEMA_TEMPLATE = new PropertyDescriptor.Builder()
            .name("Schema Template")
            .description("Apache IoTDB Schema Template defined using JSON. " +
                    "The Processor will infer the IoTDB Schema when this property is not configured. " +
                    "See the Apache IoTDB Documentation for more details: https://iotdb.apache.org/UserGuide/Master/Ecosystem-Integration/NiFi-IoTDB.html")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    private static final String ROOT_PREFIX = "root.";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        propertyDescriptors.add(PREFIX);
        propertyDescriptors.add(TIME_FIELD);
        propertyDescriptors.add(ALIGNED);
        propertyDescriptors.add(MAX_ROW_NUMBER);
        propertyDescriptors.add(RECORD_READER_FACTORY);
        propertyDescriptors.add(SCHEMA_TEMPLATE);
        return Collections.unmodifiableList(propertyDescriptors);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final String prefix = processContext.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (!prefix.startsWith(ROOT_PREFIX) || !prefix.endsWith(".")) {
            getLogger().error("Prefix does not begin with [root] and end with [.] {}", flowFile);
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String schemaProperty = processContext.getProperty(SCHEMA_TEMPLATE).evaluateAttributeExpressions(flowFile).getValue();
        final boolean aligned = processContext.getProperty(ALIGNED).evaluateAttributeExpressions(flowFile).asBoolean();
        final int maxRowNumber = processContext.getProperty(MAX_ROW_NUMBER).evaluateAttributeExpressions(flowFile).asInteger();
        final String timeField = processContext.getProperty(TIME_FIELD).evaluateAttributeExpressions(flowFile).getValue();
        final RecordReaderFactory recordParserFactory = processContext.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        try (final InputStream inputStream = processSession.read(flowFile);
             final RecordReader recordReader = recordParserFactory.createRecordReader(flowFile, inputStream, getLogger())) {
            final DatabaseSchema schema = getSchema(timeField, schemaProperty, recordReader);
            final Map<String, Tablet> tablets = generateTablets(schema, prefix, maxRowNumber);

            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                long timestamp = getTimestamp(timeField, record);
                boolean filled = false;

                for (final Map.Entry<String, Tablet> entry : tablets.entrySet()) {
                    Tablet tablet = entry.getValue();
                    int rowIndex = tablet.rowSize++;

                    tablet.addTimestamp(rowIndex, timestamp);
                    List<MeasurementSchema> measurements = tablet.getSchemas();
                    for (MeasurementSchema measurement : measurements) {
                        String id = measurement.getMeasurementId();
                        TSDataType type = measurement.getType();
                        Object value = getTypedValue(record.getValue(id), type);
                        tablet.addValue(id, rowIndex, value);
                    }
                    filled = tablet.rowSize == tablet.getMaxRowNumber();
                }
                if (filled) {
                    if (aligned) {
                        session.get().insertAlignedTablets(tablets);
                    } else {
                        session.get().insertTablets(tablets);
                    }
                    tablets.values().forEach(Tablet::reset);
                }
            }

            final AtomicBoolean remaining = new AtomicBoolean(false);
            tablets.forEach(
                    (device, tablet) -> {
                        if (!remaining.get() && tablet.rowSize != 0) {
                            remaining.set(true);
                        }
                    });
            if (remaining.get()) {
                if (aligned) {
                    session.get().insertAlignedTablets(tablets);
                } else {
                    session.get().insertTablets(tablets);
                }
            }
        } catch (final Exception e) {
            getLogger().error("Processing failed {}", flowFile, e);
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }
        processSession.transfer(flowFile, REL_SUCCESS);
    }

    private DatabaseSchema getSchema(String timeField, String property, RecordReader recordReader) throws MalformedRecordException, IOException {
        final ValidationResult result = property == null
                        ? validateSchema(timeField, recordReader.getSchema())
                        : validateSchemaAttribute(property);

        if (result.isValid()) {
            return property == null
                            ? convertSchema(timeField, recordReader.getSchema())
                            : mapper.readValue(property, DatabaseSchema.class);
        } else {
            final String message = String.format("Schema validation failed: %s", result.getMessage());
            throw new IllegalArgumentException(message);
        }
    }

    private long getTimestamp(final String timeField, final Record record) {
        final long timestamp;
        final Object time = record.getValue(timeField);
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
            throw new IllegalArgumentException(String.format("Unexpected Time Field Type: %s", time));
        }
        return timestamp;
    }

    private Object getTypedValue(Object value, TSDataType type) {
        final Object typedValue;
        if (value == null) {
            typedValue = null;
        } else {
            try {
                typedValue = convertType(value, type);
            } catch (final Exception e) {
                final String message = String.format("Value [%s] cannot be converted to the type [%s]", value, type);
                throw new IllegalArgumentException(message, e);
            }
        }
        return typedValue;
    }
}
