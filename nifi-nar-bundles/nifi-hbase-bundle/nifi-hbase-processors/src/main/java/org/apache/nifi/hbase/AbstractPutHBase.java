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
package org.apache.nifi.hbase;


import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Base class for processors that put data to HBase.
 */
public abstract class AbstractPutHBase extends AbstractProcessor {

    protected static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();
    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase Table to put data into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor ROW_ID = new PropertyDescriptor.Builder()
            .name("Row Identifier")
            .description("Specifies the Row ID to use when inserting data into HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final String STRING_ENCODING_VALUE = "String";
    static final String BYTES_ENCODING_VALUE = "Bytes";
    static final String BINARY_ENCODING_VALUE = "Binary";


    protected static final AllowableValue ROW_ID_ENCODING_STRING = new AllowableValue(STRING_ENCODING_VALUE, STRING_ENCODING_VALUE,
            "Stores the value of row id as a UTF-8 String.");
    protected static final AllowableValue ROW_ID_ENCODING_BINARY = new AllowableValue(BINARY_ENCODING_VALUE, BINARY_ENCODING_VALUE,
            "Stores the value of the rows id as a binary byte array. It expects that the row id is a binary formatted string.");

    static final PropertyDescriptor ROW_ID_ENCODING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Row Identifier Encoding Strategy")
            .description("Specifies the data type of Row ID used when inserting data into HBase. The default behavior is" +
                    " to convert the row id to a UTF-8 byte array. Choosing Binary will convert a binary formatted string" +
                    " to the correct byte[] representation. The Binary option should be used if you are using Binary row" +
                    " keys in HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(false)
            .defaultValue(ROW_ID_ENCODING_STRING.getValue())
            .allowableValues(ROW_ID_ENCODING_STRING,ROW_ID_ENCODING_BINARY)
            .build();
    protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("Column Qualifier")
            .description("The Column Qualifier to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor TIMESTAMP = new PropertyDescriptor.Builder()
            .name("timestamp")
            .displayName("Timestamp")
            .description("The timestamp for the cells being created in HBase. This field can be left blank and HBase will use the current time.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();
    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution. The FlowFiles will be " +
                    "grouped by table, and a single Put per table will be performed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in HBase")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to HBase")
            .build();

    protected HBaseClientService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        final Map<String,List<PutFlowFile>> tablePuts = new HashMap<>();

        // Group FlowFiles by HBase Table
        for (final FlowFile flowFile : flowFiles) {
            final PutFlowFile putFlowFile = createPut(session, context, flowFile);

            if (putFlowFile == null) {
                // sub-classes should log appropriate error messages before returning null
                session.transfer(flowFile, REL_FAILURE);
            } else if (!putFlowFile.isValid()) {
                if (StringUtils.isBlank(putFlowFile.getTableName())) {
                    getLogger().error("Missing table name for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (null == putFlowFile.getRow()) {
                    getLogger().error("Missing row id for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else if (putFlowFile.getColumns() == null || putFlowFile.getColumns().isEmpty()) {
                    getLogger().error("No columns provided for FlowFile {}; routing to failure", new Object[]{flowFile});
                } else {
                    // really shouldn't get here, but just in case
                    getLogger().error("Failed to produce a put for FlowFile {}; routing to failure", new Object[]{flowFile});
                }
                session.transfer(flowFile, REL_FAILURE);
            } else {
                List<PutFlowFile> putFlowFiles = tablePuts.get(putFlowFile.getTableName());
                if (putFlowFiles == null) {
                    putFlowFiles = new ArrayList<>();
                    tablePuts.put(putFlowFile.getTableName(), putFlowFiles);
                }
                putFlowFiles.add(putFlowFile);
            }
        }

        getLogger().debug("Sending {} FlowFiles to HBase in {} put operations", new Object[]{flowFiles.size(), tablePuts.size()});

        final long start = System.nanoTime();
        final List<PutFlowFile> successes = new ArrayList<>();

        for (Map.Entry<String, List<PutFlowFile>> entry : tablePuts.entrySet()) {
            try {
                clientService.put(entry.getKey(), entry.getValue());
                successes.addAll(entry.getValue());
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);

                for (PutFlowFile putFlowFile : entry.getValue()) {
                    getLogger().error("Failed to send {} to HBase due to {}; routing to failure", new Object[]{putFlowFile.getFlowFile(), e});
                    final FlowFile failure = session.penalize(putFlowFile.getFlowFile());
                    session.transfer(failure, REL_FAILURE);
                }
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        getLogger().debug("Sent {} FlowFiles to HBase successfully in {} milliseconds", new Object[]{successes.size(), sendMillis});

        for (PutFlowFile putFlowFile : successes) {
            session.transfer(putFlowFile.getFlowFile(), REL_SUCCESS);
            final String details = "Put " + putFlowFile.getColumns().size() + " cells to HBase";
            session.getProvenanceReporter().send(putFlowFile.getFlowFile(), getTransitUri(putFlowFile), details, sendMillis);
        }

    }

    protected String getTransitUri(PutFlowFile putFlowFile) {
        return "hbase://" + putFlowFile.getTableName() + "/" + new String(putFlowFile.getRow(), StandardCharsets.UTF_8);
    }

    protected byte[] getRow(final String row, final String encoding) {
        //check to see if we need to modify the rowKey before we pass it down to the PutFlowFile
        byte[] rowKeyBytes = null;
        if(BINARY_ENCODING_VALUE.contentEquals(encoding)){
            rowKeyBytes = clientService.toBytesBinary(row);
        }else{
            rowKeyBytes = row.getBytes(StandardCharsets.UTF_8);
        }
        return rowKeyBytes;
    }
    /**
     * Sub-classes provide the implementation to create a put from a FlowFile.
     *
     * @param session
     *              the current session
     * @param context
     *              the current context
     * @param flowFile
     *              the FlowFile to create a Put from
     *
     * @return a PutFlowFile instance for the given FlowFile
     */
    protected abstract PutFlowFile createPut(final ProcessSession session, final ProcessContext context, final FlowFile flowFile);

}
