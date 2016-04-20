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
package org.apache.nifi.processors.hive;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.LongHolder;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.hive.HiveJdbcCommon;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"hive", "sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("Execute provided HiveQL SELECT query against a Hive database connection. Query result will be converted to Avro format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'executehiveql.row.count' indicates how many rows were selected.")
public class ExecuteHiveQL extends AbstractHiveQLProcessor {

    public static final String RESULT_ROW_COUNT = "executehiveql.row.count";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from HiveQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("HiveQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();
    private final Set<Relationship> relationships;

    public static final PropertyDescriptor HIVEQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("HiveQL select query")
            .description("HiveQL select query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private final List<PropertyDescriptor> propDescriptors;

    public ExecuteHiveQL() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(HIVE_DBCP_SERVICE);
        pds.add(HIVEQL_SELECT_QUERY);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ProcessorLog logger = getLogger();
        final HiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(HiveDBCPService.class);
        final String selectQuery = context.getProperty(HIVEQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        final StopWatch stopWatch = new StopWatch(true);

        try (final Connection con = dbcpService.getConnection();
            final Statement st = con.createStatement()) {
            final LongHolder nrOfRows = new LongHolder(0L);
            if (fileToProcess == null) {
                fileToProcess = session.create();
            }
            fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try {
                        logger.debug("Executing query {}", new Object[]{selectQuery});
                        final ResultSet resultSet = st.executeQuery(selectQuery);
                        nrOfRows.set(HiveJdbcCommon.convertToAvroStream(resultSet, out));
                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }
                }
            });

            // set attribute how many rows were selected
            fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, nrOfRows.get().toString());

            logger.info("{} contains {} Avro records; transferring to 'success'",
                    new Object[]{fileToProcess, nrOfRows.get()});
            session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + nrOfRows.get() + " rows",
                    stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(fileToProcess, REL_SUCCESS);
        } catch (final ProcessException | SQLException e) {
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute HiveQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute HiveQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, fileToProcess, e});
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute HiveQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.transfer(fileToProcess, REL_FAILURE);
            }
        }
    }
}
