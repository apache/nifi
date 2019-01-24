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
package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"cassandra", "cql", "put", "insert", "update", "set", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This is a record aware processor that reads the content of the incoming FlowFile as individual records using the " +
        "configured 'Record Reader' and writes them to Apache Cassandra using native protocol version 3 or higher.")
public class PutCassandraRecord extends AbstractCassandraProcessor {

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-reader")
            .displayName("Record Reader")
            .description("Specifies the type of Record Reader controller service to use for parsing the incoming data " +
                    "and determining the schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-table")
            .displayName("Table name")
            .description("The name of the Cassandra table to which the records have to be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-batch-size")
            .displayName("Batch size")
            .description("Specifies the number of 'Insert statements' to be grouped together to execute as a batch (BatchStatement)")
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor BATCH_STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-batch-statement-type")
            .displayName("Batch Statement Type")
            .description("Specifies the type of 'Batch Statement' to be used.")
            .allowableValues(BatchStatement.Type.values())
            .defaultValue(BatchStatement.Type.LOGGED.toString())
            .required(false)
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractCassandraProcessor.CONSISTENCY_LEVEL)
            .allowableValues(ConsistencyLevel.SERIAL.name(), ConsistencyLevel.LOCAL_SERIAL.name())
            .defaultValue(ConsistencyLevel.SERIAL.name())
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_PROVIDER_SERVICE, CONTACT_POINTS, KEYSPACE, TABLE, CLIENT_AUTH, USERNAME, PASSWORD,
            RECORD_READER_FACTORY, BATCH_SIZE, CONSISTENCY_LEVEL, BATCH_STATEMENT_TYPE, PROP_SSL_CONTEXT_SERVICE));

    private final static Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();

        if (inputFlowFile == null) {
            return;
        }

        final String cassandraTable = context.getProperty(TABLE).evaluateAttributeExpressions(inputFlowFile).getValue();
        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final String batchStatementType = context.getProperty(BATCH_STATEMENT_TYPE).getValue();
        final String serialConsistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();

        final BatchStatement batchStatement;
        final Session connectionSession = cassandraSession.get();
        final AtomicInteger recordsAdded = new AtomicInteger(0);
        final StopWatch stopWatch = new StopWatch(true);

        boolean error = false;

        try (final InputStream inputStream = session.read(inputFlowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(inputFlowFile, inputStream, getLogger())){

            final RecordSchema schema = reader.getSchema();
            Record record;

            batchStatement = new BatchStatement(BatchStatement.Type.valueOf(batchStatementType));
            batchStatement.setSerialConsistencyLevel(ConsistencyLevel.valueOf(serialConsistencyLevel));

            while((record = reader.nextRecord()) != null) {
                Map<String, Object> recordContentMap = (Map<String, Object>) DataTypeUtils
                        .convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
                Insert insertQuery;

                if (cassandraTable.contains(".")) {
                    String keyspaceAndTable[] = cassandraTable.split("\\.");
                    insertQuery = QueryBuilder.insertInto(keyspaceAndTable[0], keyspaceAndTable[1]);
                } else {
                    insertQuery = QueryBuilder.insertInto(cassandraTable);
                }
                for (String fieldName : schema.getFieldNames()) {
                    insertQuery.value(fieldName, recordContentMap.get(fieldName));
                }
                batchStatement.add(insertQuery);

                if (recordsAdded.incrementAndGet() == batchSize) {
                    connectionSession.execute(batchStatement);
                    batchStatement.clear();
                }
            }

            if (batchStatement.size() != 0) {
                connectionSession.execute(batchStatement);
                batchStatement.clear();
            }

        } catch (Exception e) {
            error = true;
            getLogger().error("Unable to write the records into Cassandra table due to {}", new Object[] {e});
            session.transfer(inputFlowFile, REL_FAILURE);
        } finally {
            if (!error) {
                stopWatch.stop();
                long duration = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                String transitUri = "cassandra://" + connectionSession.getCluster().getMetadata().getClusterName() + "." + cassandraTable;

                session.getProvenanceReporter().send(inputFlowFile, transitUri, "Inserted " + recordsAdded.get() + " records", duration);
                session.transfer(inputFlowFile, REL_SUCCESS);
            }
        }

    }

    @OnUnscheduled
    public void stop(ProcessContext context) {
        super.stop(context);
    }

    @OnShutdown
    public void shutdown(ProcessContext context) {
        super.stop(context);
    }

}
