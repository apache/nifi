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
package org.apache.nifi.accumulo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

public class PutAccumuloCell extends AbstractProcessor {

    static final AllowableValue DURABILITY_DEFAULT = new AllowableValue(Durability.DEFAULT.name(), "System Default", "The data is stored using the Table or System's default configuration");
    static final AllowableValue DURABILITY_NONE = new AllowableValue(Durability.NONE.name(), "None", "The data is transferred to 'success' without waiting for confirmation from Accumulo");
    static final AllowableValue DURABILITY_FLUSH = new AllowableValue(Durability.FLUSH.name(), "Flush",
            "The data is transferred to 'success' after Accumulo confirms that it has received the data, but the data may not be persisted");
    static final AllowableValue DURABILITY_LOG = new AllowableValue(Durability.LOG.name(), "Log",
            "The data is transferred to 'success' after Accumulo has received the data, but the data may not yet be replicated");
    static final AllowableValue DURABILITY_SYNC = new AllowableValue(Durability.SYNC.name(), "Sync", "The data is transferred to 'success' only after Accumulo confirms that it has been stored");

    static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Accumulo Instance Name")
            .description("The name of the Accumulo Instance to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ZOOKEEPER_CONNECT_STRING = new PropertyDescriptor.Builder()
            .name("ZooKeeper Connection String")
            .description("A comma-separated list of ZooKeeper hostname:port pairs")
            .required(true)
            .defaultValue("localhost:2181")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username to use when connecting to Accumulo")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password to use when connecting to Accumulo")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table")
            .description("The table in Accumulo to Put data to")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor ROW_ID = new PropertyDescriptor.Builder()
            .name("Row Identifier")
            .description("The identifier to use for the row to Put to Accumulo")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to Put to Accumulo")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("Column Qualifier")
            .description("The Column Qualifier to Put to Accumulo")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor VISIBILITY = new PropertyDescriptor.Builder()
            .name("Visibility")
            .description("The visibility for this Accumulo cell")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("Specifies how long to wait without receiving a response from Accumulo before routing FlowFiles to 'failure'")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Number of FlowFiles to send in a single batch. Accumulo does not provide information about which data fails in the case of a batch operation. "
                    + "Therefore, if any FlowFile fails in the batch, all may be routed to failure, even if they were already sent successfully to Accumulo. "
                    + "If this is problematic for your use case, use a Batch Size of 1.")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor DURABILITY = new PropertyDescriptor.Builder()
            .name("Data Durability")
            .description("Specifies how durably the data must be stored on Accumulo before sending a FlowFile to the 'success' relationship.")
            .required(true)
            .allowableValues(DURABILITY_DEFAULT, DURABILITY_NONE, DURABILITY_FLUSH, DURABILITY_LOG, DURABILITY_SYNC)
            .defaultValue(DURABILITY_DEFAULT.getValue())
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been sent to Accumulo")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Accumulo")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(INSTANCE_NAME);
        properties.add(ZOOKEEPER_CONNECT_STRING);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(TABLE);
        properties.add(ROW_ID);
        properties.add(COLUMN_FAMILY);
        properties.add(COLUMN_QUALIFIER);
        properties.add(VISIBILITY);
        properties.add(DURABILITY);
        properties.add(BATCH_SIZE);
        properties.add(TIMEOUT);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    private Connector getConnector(final ProcessContext context) throws AccumuloException, AccumuloSecurityException {
        final String instanceName = context.getProperty(INSTANCE_NAME).getValue();
        final String zookeeperConnString = context.getProperty(ZOOKEEPER_CONNECT_STRING).getValue();
        final Instance instance = new ZooKeeperInstance(instanceName, zookeeperConnString);

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        return instance.getConnector(username, new PasswordToken(password));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).asInteger());
        if (flowFiles.isEmpty()) {
            return;
        }

        try {
            final String durability = context.getProperty(DURABILITY).getValue();

            final Connector connector = getConnector(context);
            final BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
            batchWriterConfig.setDurability(Durability.valueOf(durability.toUpperCase()));
            batchWriterConfig.setTimeout(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);

            final List<FlowFile> success = new ArrayList<>(flowFiles.size());

            final MultiTableBatchWriter writer = connector.createMultiTableBatchWriter(batchWriterConfig);
            try {
                for (final FlowFile flowFile : flowFiles) {
                    final String tableName = context.getProperty(TABLE).evaluateAttributeExpressions(flowFile).getValue();

                    final BatchWriter batchWriter;
                    try {
                        batchWriter = writer.getBatchWriter(tableName);
                    } catch (final TableNotFoundException e) {
                        getLogger().error("Failed to send {} to Accumulo because the table {} is not known; routing to failure", new Object[]{flowFile, tableName});
                        session.transfer(flowFile, REL_FAILURE);
                        continue;
                    }

                    success.add(flowFile);

                    final String row = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
                    final String colFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
                    final String colQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
                    final String visibility = context.getProperty(VISIBILITY).evaluateAttributeExpressions(flowFile).getValue();

                    final byte[] content = new byte[(int) flowFile.getSize()];
                    session.read(flowFile, new InputStreamCallback() {
                        @Override
                        public void process(final InputStream in) throws IOException {
                            StreamUtils.fillBuffer(in, content);
                        }
                    });

                    final Mutation mutation = new Mutation(row.getBytes(StandardCharsets.UTF_8));
                    if (visibility == null) {
                        mutation.put(colFamily, colQualifier, new Value(content));
                    } else {
                        mutation.put(colFamily, colQualifier, new ColumnVisibility(visibility), new Value(content));
                    }
                    batchWriter.addMutation(mutation);
                    session.getProvenanceReporter().send(flowFile, "accumulo://" + tableName + "/" + row + "/" + colFamily + ":" + colQualifier);
                }
            } finally {
                writer.close();
            }

            getLogger().info("Successfully transferred {} FlowFiles to success", new Object[]{success.size()});
            session.transfer(success, REL_SUCCESS);
        } catch (final AccumuloException | AccumuloSecurityException e) {
            getLogger().error("Failed to send {} FlowFiles to Accumulo due to {}; routing to failure", new Object[]{flowFiles.size(), e});
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }
    }
}
