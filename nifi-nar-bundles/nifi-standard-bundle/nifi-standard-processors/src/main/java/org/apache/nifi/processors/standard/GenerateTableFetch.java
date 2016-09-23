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
package org.apache.nifi.processors.standard;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;


@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database", "fetch", "generate"})
@SeeAlso({QueryDatabaseTable.class, ExecuteSQL.class})
@CapabilityDescription("Generates SQL select queries that fetch \"pages\" of rows from a table. The partition size property, along with the table's row count, "
        + "determine the size and number of pages and generated FlowFiles. In addition, incremental fetching can be achieved by setting Maximum-Value Columns, "
        + "which causes the processor to track the columns' maximum values, thus only fetching rows whose columns' values exceed the observed maximums. This "
        + "processor is intended to be run on the Primary Node only.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
public class GenerateTableFetch extends AbstractDatabaseFetchProcessor {

    public static final PropertyDescriptor PARTITION_SIZE = new PropertyDescriptor.Builder()
            .name("gen-table-fetch-partition-size")
            .displayName("Partition Size")
            .description("The number of result rows to be fetched by each generated SQL statement. The total number of rows in "
                    + "the table divided by the partition size gives the number of SQL statements (i.e. FlowFiles) generated. A "
                    + "value of zero indicates that a single FlowFile is to be generated whose SQL statement will fetch all rows "
                    + "in the table.")
            .defaultValue("10000")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public GenerateTableFetch() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(DB_TYPE);
        pds.add(TABLE_NAME);
        pds.add(COLUMN_NAMES);
        pds.add(MAX_VALUE_COLUMN_NAMES);
        pds.add(QUERY_TIMEOUT);
        pds.add(PARTITION_SIZE);
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

    @OnScheduled
    public void setup(final ProcessContext context) {
        super.setup(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();
        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).getValue();
        final int partitionSize = context.getProperty(PARTITION_SIZE).asInteger();

        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            logger.error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }
        try {
            // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
            // set as the current state map (after the session has been committed)
            final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

            // Build a WHERE clause with maximum-value columns (if they exist), and a list of column names that will contain MAX(<column>) aliases. The
            // executed SQL query will retrieve the count of all records after the filter(s) have been applied, as well as the new maximum values for the
            // specified columns. This allows the processor to generate the correctly partitioned SQL statements as well as to update the state with the
            // latest observed maximum values.
            String whereClause = null;
            List<String> maxValueColumnNameList = StringUtils.isEmpty(maxValueColumnNames)
                    ? new ArrayList<>(0)
                    : Arrays.asList(maxValueColumnNames.split("\\s*,\\s*"));
            List<String> maxValueClauses = new ArrayList<>(maxValueColumnNameList.size());

            String columnsClause = null;
            List<String> maxValueSelectColumns = new ArrayList<>(maxValueColumnNameList.size() + 1);
            maxValueSelectColumns.add("COUNT(*)");

            // For each maximum-value column, get a WHERE filter and a MAX(column) alias
            IntStream.range(0, maxValueColumnNameList.size()).forEach((index) -> {
                String colName = maxValueColumnNameList.get(index);
                maxValueSelectColumns.add("MAX(" + colName + ") " + colName);
                String maxValue = statePropertyMap.get(colName.toLowerCase());
                if (!StringUtils.isEmpty(maxValue)) {
                    Integer type = columnTypeMap.get(colName.toLowerCase());
                    if (type == null) {
                        // This shouldn't happen as we are populating columnTypeMap when the processor is scheduled.
                        throw new IllegalArgumentException("No column type found for: " + colName);
                    }
                    // Add a condition for the WHERE clause
                    maxValueClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue, dbAdapter.getName()));
                }
            });

            whereClause = StringUtils.join(maxValueClauses, " AND ");
            columnsClause = StringUtils.join(maxValueSelectColumns, ", ");

            // Build a SELECT query with maximum-value columns (if present)
            final String selectQuery = dbAdapter.getSelectStatement(tableName, columnsClause, whereClause, null, null, null);
            int rowCount = 0;

            try (final Connection con = dbcpService.getConnection();
                 final Statement st = con.createStatement()) {

                final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
                st.setQueryTimeout(queryTimeout); // timeout in seconds

                logger.debug("Executing {}", new Object[]{selectQuery});
                ResultSet resultSet;

                resultSet = st.executeQuery(selectQuery);

                if (resultSet.next()) {
                    // Total row count is in the first column
                    rowCount = resultSet.getInt(1);

                    // Update the state map with the newly-observed maximum values
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    for (int i = 2; i <= rsmd.getColumnCount(); i++) {
                        String resultColumnName = rsmd.getColumnName(i).toLowerCase();
                        int type = rsmd.getColumnType(i);
                        try {
                            String newMaxValue = getMaxValueFromRow(resultSet, i, type, statePropertyMap.get(resultColumnName.toLowerCase()), dbAdapter.getName());
                            if (newMaxValue != null) {
                                statePropertyMap.put(resultColumnName, newMaxValue);
                            }
                        } catch (ParseException | IOException pie) {
                            // Fail the whole thing here before we start creating flow files and such
                            throw new ProcessException(pie);
                        }
                    }
                } else {
                    // Something is very wrong here, one row (even if count is zero) should be returned
                    throw new SQLException("No rows returned from metadata query: " + selectQuery);
                }
            } catch (SQLException e) {
                logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
                throw new ProcessException(e);
            }
            final int numberOfFetches = (partitionSize == 0) ? rowCount : (rowCount / partitionSize) + (rowCount % partitionSize == 0 ? 0 : 1);


            // Generate SQL statements to read "pages" of data
            for (int i = 0; i < numberOfFetches; i++) {
                FlowFile sqlFlowFile;

                Integer limit = partitionSize == 0 ? null : partitionSize;
                Integer offset = partitionSize == 0 ? null : i * partitionSize;
                final String query = dbAdapter.getSelectStatement(tableName, columnNames, StringUtils.join(maxValueClauses, " AND "), null, limit, offset);
                sqlFlowFile = session.create();
                sqlFlowFile = session.write(sqlFlowFile, out -> {
                    out.write(query.getBytes());
                });
                session.transfer(sqlFlowFile, REL_SUCCESS);
            }

            session.commit();
            try {
                // Update the state
                stateManager.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                logger.error("{} failed to update State Manager, observed maximum values will not be recorded. "
                                + "Also, any generated SQL statements may be duplicated.",
                        new Object[]{this, ioe});
            }
        } catch (final ProcessException pe) {
            // Log the cause of the ProcessException if it is available
            Throwable t = (pe.getCause() == null ? pe : pe.getCause());
            logger.error("Error during processing: {}", new Object[]{t.getMessage()}, t);
            session.rollback();
            context.yield();
        }
    }
}
