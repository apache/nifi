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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.SimpleRecordSchema;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

@EventDriven
@SupportsBatching
@Tags({"iotdb", "insert", "tablet"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription(
        "This is a processor that reads the sql query from the incoming FlowFile and using it to " +
                "query the result from IoTDB using native interface." +
                "Then it use the configured 'Record Writer' to generate the flowfile ")
public class QueryIoTDBRecord extends AbstractIoTDB {

    public static final String IOTDB_EXECUTED_QUERY = "iotdb.executed.query";

    private static final int DEFAULT_IOTDB_FETCH_SIZE = 10000;

    public static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing results to a FlowFile. The Record Writer may use Inherit Schema to emulate the inferred schema behavior, i.e. "
                    + "an explicit schema need not be defined in the writer, and will be supplied by the same logic used to infer the schema from the column types.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor IOTDB_QUERY = new PropertyDescriptor.Builder()
            .name("query")
            .displayName("Query")
            .description("The IoTDB query to execute. "
                    + "Note: If there are incoming connections, then the query is created from incoming FlowFile's content otherwise"
                    + " it is created from this property.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Integer FETCH_SIZE = 100000;
    public static final PropertyDescriptor IOTDB_QUERY_FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("iotdb-query-chunk-size")
            .displayName("Fetch Size")
            .description("Chunking can be used to return results in a stream of smaller batches "
                    + "(each has a partial results up to a chunk size) rather than as a single response. "
                    + "Chunking queries can return an unlimited number of rows. Note: Chunking is enable when result chunk size is greater than 0")
            .defaultValue(String.valueOf(DEFAULT_IOTDB_FETCH_SIZE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createLongValidator(0, FETCH_SIZE, true))
            .required(true)
            .build();

    private static RecordSetWriterFactory recordSetWriterFactory;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        propertyDescriptors.add(IOTDB_QUERY);
        propertyDescriptors.add(IOTDB_QUERY_FETCH_SIZE);
        propertyDescriptors.add(RECORD_WRITER_FACTORY);
        return Collections.unmodifiableList(propertyDescriptors);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IoTDBConnectionException {
        super.onScheduled(context);
        // Either input connection or scheduled query is required
        if (!context.getProperty(IOTDB_QUERY).isSet()
                && ! context.hasIncomingConnection()) {
            throw new ProcessException("The IoTDB Query processor requires input connection or scheduled IoTDB query");
        }
        recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) throws ProcessException {

        String query;
        Charset charset;
        FlowFile outgoingFlowFile;
        int fetchSize = DEFAULT_IOTDB_FETCH_SIZE;
        // If there are incoming connections, prepare query params from flow file
        if (context.hasIncomingConnection()) {
            FlowFile incomingFlowFile = processSession.get();

            if ( incomingFlowFile == null && context.hasNonLoopConnection() ) {
                return;
            }
            fetchSize = context.getProperty(IOTDB_QUERY_FETCH_SIZE).evaluateAttributeExpressions(incomingFlowFile).asInteger();
            charset = Charset.forName("UTF-8");
            if ( (incomingFlowFile != null ? incomingFlowFile.getSize() : 0) == 0 ) {
                if ( context.getProperty(IOTDB_QUERY).isSet() ) {
                    query = context.getProperty(IOTDB_QUERY).evaluateAttributeExpressions(incomingFlowFile).getValue();
                } else {
                    String message = "FlowFile query is empty and no scheduled query is set";
                    getLogger().error(message);
                    incomingFlowFile = processSession.putAttribute(incomingFlowFile, "iotdb.error.message", message);
                    processSession.transfer(incomingFlowFile, REL_FAILURE);
                    return;
                }
            } else {
                try {
                    query = getQuery(processSession, charset, incomingFlowFile);
                } catch(IOException ioe) {
                    getLogger().error("Exception while reading from FlowFile " + ioe.getLocalizedMessage(), ioe);
                    throw new ProcessException(ioe);
                }
            }
            outgoingFlowFile = incomingFlowFile;
        } else {
            outgoingFlowFile = processSession.create();
            query = context.getProperty(IOTDB_QUERY).evaluateAttributeExpressions(outgoingFlowFile).getValue();
        }

        try {
            SessionDataSet dataSet = session.get().executeQueryStatement(query);
            List<Map<String,Object>> result=new ArrayList<>();
            dataSet.setFetchSize(fetchSize);
            List<String> fieldType = dataSet.getColumnTypes();
            List<String> fieldNames = dataSet.getColumnNames();
            List<RecordField> recordFields = new ArrayList<>();
            for(int i = 0; i< fieldNames.size(); i++ ){
                recordFields.add(new RecordField(fieldNames.get(i), getType(fieldType.get(i)).getDataType()));
            }
            final RecordSchema schema = new SimpleRecordSchema(recordFields);
            PipedInputStream inputStream = new PipedInputStream();
            final PipedOutputStream outputStream = new PipedOutputStream(inputStream);
            RecordSetWriter resultSetWriter =
                    recordSetWriterFactory.createWriter(getLogger(),schema,outputStream,outgoingFlowFile);
            List<Record> records = new ArrayList<>();
            while (dataSet.hasNext()) {
                Map<String,Object> map = new HashMap<String,Object>();
                RowRecord rowRecord = dataSet.next();
                map.put(fieldNames.get(0), rowRecord.getTimestamp()); //Put the timestamp
                List<Field> fields = rowRecord.getFields();
                for(int i = 0; i< fieldNames.size() - 1; i++ ){ //Put the rest data
                    TSDataType dataType = fields.get(i).getDataType();
                    map.put(fieldNames.get(i+1), fields.get(i).getObjectValue(dataType));
                }
                result.add(map);
                Record record = new MapRecord(schema, map);
                resultSetWriter.write(record);
                records.add(record);
            }

            if ( getLogger().isDebugEnabled() ) {
                getLogger().debug("Query result {} ", result);
            }
            resultSetWriter.close();
            processSession.importFrom(inputStream, outgoingFlowFile);
            outgoingFlowFile = processSession.putAttribute(outgoingFlowFile, IOTDB_EXECUTED_QUERY, String.valueOf(query));
            processSession.transfer(outgoingFlowFile, REL_SUCCESS);
        } catch (Exception exception) {
            outgoingFlowFile = populateErrorAttributes(processSession, outgoingFlowFile, query, exception.getMessage());
            getLogger().error("IoTDB query failed", exception);
            processSession.transfer(outgoingFlowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected String getQuery(final ProcessSession processSession, Charset charset, FlowFile incomingFlowFile)
            throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        processSession.exportTo(incomingFlowFile, outputStream);
        outputStream.close();

        return outputStream.toString(StandardCharsets.UTF_8);
    }

    protected FlowFile populateErrorAttributes(final ProcessSession processSession, FlowFile flowFile, String query,
                                               String message) {
        Map<String,String> attributes = new HashMap<>();
        attributes.put("iotdb.error.message", String.valueOf(message));
        attributes.put(IOTDB_EXECUTED_QUERY, String.valueOf(query));
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }
}