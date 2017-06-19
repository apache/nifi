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

package org.apache.nifi.processors.kudu;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
//import org.apache.nifi.serialization.RecordReaderFactory;

import java.util.List;

/**
 * Created by Cam Mach - Inspur USA on 5/23/17.
 */
public abstract class AbstractKudu extends AbstractProcessor {

    protected static final PropertyDescriptor KUDU_MASTERS = new PropertyDescriptor.Builder()
            .name("KUDU Masters")
            .description("List all kudu masters's ip with port (7051), comma separated")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Kudu Table to put data into")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading records from incoming flow files.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution. The FlowFiles will be " +
                    "grouped by table, and a single Put per table will be performed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in Kudu")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Kudu")
            .build();

    protected String kuduMasters;
    protected String tableName;
    protected int batchSize;

    protected KuduClient kuduClient = null;
    protected KuduTable kuduTable = null;
    protected KuduSession kuduSession = null;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            tableName = context.getProperty(TABLE_NAME).getValue();
            kuduMasters = context.getProperty(KUDU_MASTERS).getValue();
            batchSize = context.getProperty(BATCH_SIZE).asInteger();

            getLogger().debug("onScheduled tableName: " + tableName + " - Kudu Master: " + kuduMasters);

            kuduClient = (kuduClient == null) ? getKuduConnection(kuduMasters) : kuduClient;
            kuduTable = (kuduTable == null) ? kuduClient.openTable(tableName) : kuduTable;
            kuduSession = (kuduSession == null) ? kuduClient.newSession() : kuduSession;
        } catch (KuduException ex) {
            getLogger().error(ex.getMessage());
        }
    }

    @OnStopped
    public final void closeClient() throws KuduException {
        if (kuduClient != null) {
            getLogger().info("Closing KuduClient");
            kuduClient.close();
            kuduClient = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            List<FlowFile> flowFiles = session.get(batchSize);
            if (flowFiles == null || flowFiles.size() == 0) {
                return;
            }

            for (final FlowFile flowFile : flowFiles) {
                final OperationResponse putFlowFile = createPut(session, context, flowFile);

                if (putFlowFile == null) {
                    session.transfer(flowFile, REL_FAILURE);
                } else if (putFlowFile.hasRowError()) {
                    getLogger().error("Failed to produce a put for FlowFile {}; routing to failure", new Object[]{flowFile});
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().send(flowFile, "no transit url. Successfully added flowfile to kudu");
                }
            }
        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
        }
    }

    protected KuduClient getKuduConnection(String masters) {
        return new KuduClient.KuduClientBuilder(kuduMasters).build();
    }

    protected abstract OperationResponse createPut(final ProcessSession session, final ProcessContext context, final FlowFile flowFile);
}
