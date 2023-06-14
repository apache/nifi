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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.SimpleRecordSchema;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

@SupportsBatching
@Tags({"IoT", "Timeseries"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Query Apache IoTDB and write results as Records")
@WritesAttributes({
        @WritesAttribute(attribute = QueryIoTDBRecord.IOTDB_ERROR_MESSAGE, description = "Error message written on query failures"),
        @WritesAttribute(attribute = QueryIoTDBRecord.MIME_TYPE, description = "Content Type based on configured Record Set Writer")
})
public class QueryIoTDBRecord extends AbstractIoTDB {

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("Query")
            .displayName("Query")
            .description("IoTDB query to be executed")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .displayName("Fetch Size")
            .description("Maximum number of results to return in a single chunk. Configuring 1 or more enables result set chunking")
            .defaultValue(String.valueOf(10_000))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createLongValidator(0, 100_000, true))
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("Service for writing IoTDB query results as records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final String IOTDB_ERROR_MESSAGE = "iotdb.error.message";

    public static final String MIME_TYPE = "mime.type";

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        propertyDescriptors.add(QUERY);
        propertyDescriptors.add(FETCH_SIZE);
        propertyDescriptors.add(RECORD_WRITER_FACTORY);
        return Collections.unmodifiableList(propertyDescriptors);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
        final int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);

        try (
                SessionDataSet sessionDataSet = this.session.get().executeQueryStatement(query);
                OutputStream outputStream = session.write(flowFile)
        ) {
            sessionDataSet.setFetchSize(fetchSize);

            final RecordSchema recordSchema = getRecordSchema(sessionDataSet);
            final RecordSetWriter recordSetWriter = recordSetWriterFactory.createWriter(getLogger(), recordSchema, outputStream, flowFile);
            while (sessionDataSet.hasNext()) {
                final RowRecord rowRecord = sessionDataSet.next();
                final Record record = getRecord(recordSchema, rowRecord);
                recordSetWriter.write(record);
            }

            recordSetWriter.close();
            flowFile = session.putAttribute(flowFile, MIME_TYPE, recordSetWriter.getMimeType());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            flowFile = session.putAttribute(flowFile, IOTDB_ERROR_MESSAGE, e.getMessage());
            getLogger().error("IoTDB query failed {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private Record getRecord(final RecordSchema schema, final RowRecord rowRecord) {
        final Map<String, Object> row = new LinkedHashMap<>();
        final Iterator<String> recordFieldNames = schema.getFieldNames().iterator();

        // Put Timestamp as first field
        row.put(recordFieldNames.next(), rowRecord.getTimestamp());

        final Iterator<Field> rowRecordFields = rowRecord.getFields().iterator();
        while (recordFieldNames.hasNext()) {
            final String recordFieldName = recordFieldNames.next();
            if (rowRecordFields.hasNext()) {
                final Field rowRecordField = rowRecordFields.next();
                final TSDataType dataType = rowRecordField.getDataType();
                final Object objectValue = rowRecordField.getObjectValue(dataType);
                row.put(recordFieldName, objectValue);
            }
        }
        return new MapRecord(schema, row);
    }

    private RecordSchema getRecordSchema(final SessionDataSet sessionDataSet) {
        final Iterator<String> columnTypes = sessionDataSet.getColumnTypes().iterator();
        final Iterator<String> columnNames = sessionDataSet.getColumnNames().iterator();

        final List<RecordField> recordFields = new ArrayList<>();
        while (columnNames.hasNext()) {
            final String recordFieldName = columnNames.next();
            final String columnType = columnTypes.next();
            final RecordFieldType recordFieldType = getType(columnType);
            final DataType recordDataType = recordFieldType.getDataType();
            final RecordField recordField = new RecordField(recordFieldName, recordDataType);
            recordFields.add(recordField);
        }
        return new SimpleRecordSchema(recordFields);
    }
}