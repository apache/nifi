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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase"})
@CapabilityDescription("Adds the Contents of a FlowFile to HBase as the value of a single cell")
public class PutHBaseCell extends AbstractProcessor {

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
    static final PropertyDescriptor ROW = new PropertyDescriptor.Builder()
            .name("Row Identifier")
            .description("Specifies the Row ID to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("Column Qualifier")
            .description("The Column Qualifier to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution. The FlowFiles will be " +
                    "grouped by table, and a single Put per table will be performed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in HBase")
            .build();
    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to HBase")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW);
        properties.add(COLUMN_FAMILY);
        properties.add(COLUMN_QUALIFIER);
        properties.add(BATCH_SIZE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(FAILURE);
        return rels;
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
            final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            final String row = context.getProperty(ROW).evaluateAttributeExpressions(flowFile).getValue();
            final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
            final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();

            if (StringUtils.isBlank(tableName) || StringUtils.isBlank(row) || StringUtils.isBlank(columnFamily) || StringUtils.isBlank(columnQualifier)) {
                getLogger().error("Invalid FlowFile {} missing table, row, column familiy, or column qualifier; routing to failure", new Object[]{flowFile});
                session.transfer(flowFile, FAILURE);
            } else {
                final byte[] buffer = new byte[(int) flowFile.getSize()];
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        StreamUtils.fillBuffer(in, buffer);
                    }
                });

                final PutFlowFile putFlowFile = new PutFlowFile(tableName, row, columnFamily, columnQualifier, buffer, flowFile);

                List<PutFlowFile> putFlowFiles = tablePuts.get(tableName);
                if (putFlowFiles == null) {
                    putFlowFiles = new ArrayList<>();
                    tablePuts.put(tableName, putFlowFiles);
                }
                putFlowFiles.add(putFlowFile);
            }
        }

        getLogger().debug("Sending {} FlowFiles to HBase in {} put operations", new Object[] {flowFiles.size(), tablePuts.size()});

        final long start = System.nanoTime();
        final List<PutFlowFile> successes = new ArrayList<>();
        final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        for (Map.Entry<String, List<PutFlowFile>> entry : tablePuts.entrySet()) {
            try {
                hBaseClientService.put(entry.getKey(), entry.getValue());
                successes.addAll(entry.getValue());
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);

                for (PutFlowFile putFlowFile : entry.getValue()) {
                    getLogger().error("Failed to send {} to HBase due to {}; routing to failure", new Object[]{putFlowFile.getFlowFile(), e});
                    final FlowFile failure = session.penalize(putFlowFile.getFlowFile());
                    session.transfer(failure, FAILURE);
                }
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        getLogger().debug("Sent {} FlowFiles to HBase successfully in {} milliseconds", new Object[] {successes.size(), sendMillis});

        for (PutFlowFile putFlowFile : successes) {
            session.transfer(putFlowFile.getFlowFile(), REL_SUCCESS);
            session.getProvenanceReporter().send(putFlowFile.getFlowFile(), getTransitUri(putFlowFile), sendMillis);
        }

    }

    protected String getTransitUri(PutFlowFile putFlowFile) {
        return "hbase://" + putFlowFile.getTableName() + "/" + putFlowFile.getRow() + "/" + putFlowFile.getColumnFamily()
                + ":" + putFlowFile.getColumnQualifier();
    }

}
