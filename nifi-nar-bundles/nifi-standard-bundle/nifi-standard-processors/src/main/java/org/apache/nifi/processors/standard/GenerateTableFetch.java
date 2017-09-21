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
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;


@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "jdbc", "query", "database", "fetch", "generate"})
@SeeAlso({QueryDatabaseTable.class, ExecuteSQL.class, ListDatabaseTables.class})
@CapabilityDescription("Generates SQL select queries that fetch \"pages\" of rows from a table. The partition size property, along with the table's row count, "
        + "determine the size and number of pages and generated FlowFiles. In addition, incremental fetching can be achieved by setting Maximum-Value Columns, "
        + "which causes the processor to track the columns' maximum values, thus only fetching rows whose columns' values exceed the observed maximums. This "
        + "processor is intended to be run on the Primary Node only.\n\n"
        + "This processor can accept incoming connections; the behavior of the processor is different whether incoming connections are provided:\n"
        + "  - If no incoming connection(s) are specified, the processor will generate SQL queries on the specified processor schedule. Expression Language is supported for many "
        + "fields, but no flow file attributes are available. However the properties will be evaluated using the Variable Registry.\n"
        + "  - If incoming connection(s) are specified and no flow file is available to a processor task, no work will be performed.\n"
        + "  - If incoming connection(s) are specified and a flow file is available to a processor task, the flow file's attributes may be used in Expression Language for such fields "
        + "as Table Name and others. However, the Max-Value Columns and Columns to Return fields must be empty or refer to columns that are available in each specified table.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttributes({
        @WritesAttribute(attribute = "generatetablefetch.sql.error", description = "If the processor has incoming connections, and processing an incoming flow file causes "
                + "a SQL Exception, the flow file is routed to failure and this attribute is set to the exception message."),
        @WritesAttribute(attribute = "generatetablefetch.tableName", description = "The name of the database table to be queried."),
        @WritesAttribute(attribute = "generatetablefetch.columnNames", description = "The comma-separated list of column names used in the query."),
        @WritesAttribute(attribute = "generatetablefetch.whereClause", description = "Where clause used in the query to get the expected rows."),
        @WritesAttribute(attribute = "generatetablefetch.maxColumnNames", description = "The comma-separated list of column names used to keep track of data "
                + "that has been returned since the processor started running."),
        @WritesAttribute(attribute = "generatetablefetch.limit", description = "The number of result rows to be fetched by the SQL statement."),
        @WritesAttribute(attribute = "generatetablefetch.offset", description = "Offset to be used to retrieve the corresponding partition.")
})
@DynamicProperty(name = "Initial Max Value", value = "Attribute Expression Language", supportsExpressionLanguage = false, description = "Specifies an initial "
        + "max value for max value columns. Properties should be added in the format `initial.maxvalue.{max_value_column}`.")
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
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("This relationship is only used when SQL query execution (using an incoming FlowFile) failed. The incoming FlowFile will be penalized and routed to this relationship. "
                    + "If no incoming connection(s) are specified, this relationship is unused.")
            .build();

    public GenerateTableFetch() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(DB_TYPE);
        pds.add(TABLE_NAME);
        pds.add(COLUMN_NAMES);
        pds.add(MAX_VALUE_COLUMN_NAMES);
        pds.add(QUERY_TIMEOUT);
        pds.add(PARTITION_SIZE);
        pds.add(WHERE_CLAUSE);
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return super.customValidate(validationContext);
    }

    @Override
    @OnScheduled
    public void setup(final ProcessContext context) {
        maxValueProperties = getDefaultMaxValueProperties(context.getProperties());
        if (!isDynamicTableName && !isDynamicMaxValues) {
            super.setup(context);
        }
        if (context.hasIncomingConnection() && !context.hasNonLoopConnection()) {
            getLogger().error("The failure relationship can be used only if there is another incoming connection to this processor.");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();

        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            if (fileToProcess == null) {
                // Incoming connection with no flow file available, do no work (see capability description)
                return;
            }
        }

        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(fileToProcess).getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).evaluateAttributeExpressions(fileToProcess).getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions(fileToProcess).getValue();
        final int partitionSize = context.getProperty(PARTITION_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final String customWhereClause = context.getProperty(WHERE_CLAUSE).evaluateAttributeExpressions(fileToProcess).getValue();

        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;
        FlowFile finalFileToProcess = fileToProcess;


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

            // If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
            for (final Map.Entry<String, String> maxProp : maxValueProperties.entrySet()) {
                String maxPropKey = maxProp.getKey().toLowerCase();
                String fullyQualifiedMaxPropKey = getStateKey(tableName, maxPropKey);
                if (!statePropertyMap.containsKey(fullyQualifiedMaxPropKey)) {
                    String newMaxPropValue;
                    // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                    // the value has been stored under a key that is only the column name. Fall back to check the column name,
                    // but store the new initial max value under the fully-qualified key.
                    if (statePropertyMap.containsKey(maxPropKey)) {
                        newMaxPropValue = statePropertyMap.get(maxPropKey);
                    } else {
                        newMaxPropValue = maxProp.getValue();
                    }
                    statePropertyMap.put(fullyQualifiedMaxPropKey, newMaxPropValue);
                }
            }

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
                String maxValue = getColumnStateMaxValue(tableName, statePropertyMap, colName);
                if (!StringUtils.isEmpty(maxValue)) {
                    if(columnTypeMap.isEmpty()){
                        // This means column type cache is clean after instance reboot. We should re-cache column type
                        super.setup(context, false, finalFileToProcess);
                    }
                    Integer type = getColumnType(tableName, colName);

                    // Add a condition for the WHERE clause
                    maxValueClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue, dbAdapter.getName()));
                }
            });

            if (customWhereClause != null) {
                // adding the custom WHERE clause (if defined) to the list of existing clauses.
                maxValueClauses.add("(" + customWhereClause + ")");
            }

            whereClause = StringUtils.join(maxValueClauses, " AND ");
            columnsClause = StringUtils.join(maxValueSelectColumns, ", ");

            // Build a SELECT query with maximum-value columns (if present)
            final String selectQuery = dbAdapter.getSelectStatement(tableName, columnsClause, whereClause, null, null, null);
            long rowCount = 0;

            try (final Connection con = dbcpService.getConnection();
                 final Statement st = con.createStatement()) {

                final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
                st.setQueryTimeout(queryTimeout); // timeout in seconds

                logger.debug("Executing {}", new Object[]{selectQuery});
                ResultSet resultSet;

                resultSet = st.executeQuery(selectQuery);

                if (resultSet.next()) {
                    // Total row count is in the first column
                    rowCount = resultSet.getLong(1);

                    // Update the state map with the newly-observed maximum values
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    for (int i = 2; i <= rsmd.getColumnCount(); i++) {
                        //Some JDBC drivers consider the columns name and label to be very different things.
                        // Since this column has been aliased lets check the label first,
                        // if there is no label we'll use the column name.
                        String resultColumnName = (StringUtils.isNotEmpty(rsmd.getColumnLabel(i)) ? rsmd.getColumnLabel(i) : rsmd.getColumnName(i)).toLowerCase();
                        String fullyQualifiedStateKey = getStateKey(tableName, resultColumnName);
                        String resultColumnCurrentMax = statePropertyMap.get(fullyQualifiedStateKey);
                        if (StringUtils.isEmpty(resultColumnCurrentMax) && !isDynamicTableName) {
                            // If we can't find the value at the fully-qualified key name and the table name is static, it is possible (under a previous scheme)
                            // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                            // maximum value is observed, it will be stored under the fully-qualified key from then on.
                            resultColumnCurrentMax = statePropertyMap.get(resultColumnName);
                        }

                        int type = rsmd.getColumnType(i);
                        if (isDynamicTableName) {
                            // We haven't pre-populated the column type map if the table name is dynamic, so do it here
                            columnTypeMap.put(fullyQualifiedStateKey, type);
                        }
                        try {
                            String newMaxValue = getMaxValueFromRow(resultSet, i, type, resultColumnCurrentMax, dbAdapter.getName());
                            if (newMaxValue != null) {
                                statePropertyMap.put(fullyQualifiedStateKey, newMaxValue);
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

                // for each maximum-value column get a right bounding WHERE condition
                IntStream.range(0, maxValueColumnNameList.size()).forEach((index) -> {
                    String colName = maxValueColumnNameList.get(index);

                    maxValueSelectColumns.add("MAX(" + colName + ") " + colName);
                    String maxValue = getColumnStateMaxValue(tableName, statePropertyMap, colName);
                    if (!StringUtils.isEmpty(maxValue)) {
                        if(columnTypeMap.isEmpty()){
                            // This means column type cache is clean after instance reboot. We should re-cache column type
                            super.setup(context, false, finalFileToProcess);
                        }
                        Integer type = getColumnType(tableName, colName);

                        // Add a condition for the WHERE clause
                        maxValueClauses.add(colName + " <= " + getLiteralByType(type, maxValue, dbAdapter.getName()));
                    }
                });

                //Update WHERE list to include new right hand boundaries
                whereClause = StringUtils.join(maxValueClauses, " AND ");

                final long numberOfFetches = (partitionSize == 0) ? 1 : (rowCount / partitionSize) + (rowCount % partitionSize == 0 ? 0 : 1);

                // Generate SQL statements to read "pages" of data
                for (long i = 0; i < numberOfFetches; i++) {
                    Long limit = partitionSize == 0 ? null : (long) partitionSize;
                    Long offset = partitionSize == 0 ? null : i * partitionSize;
                    final String maxColumnNames = StringUtils.join(maxValueColumnNameList, ", ");
                    final String query = dbAdapter.getSelectStatement(tableName, columnNames, whereClause, maxColumnNames, limit, offset);
                    FlowFile sqlFlowFile = (fileToProcess == null) ? session.create() : session.create(fileToProcess);
                    sqlFlowFile = session.write(sqlFlowFile, out -> out.write(query.getBytes()));
                    sqlFlowFile = session.putAttribute(sqlFlowFile, "generatetablefetch.tableName", tableName);
                    if (columnNames != null) {
                        sqlFlowFile = session.putAttribute(sqlFlowFile, "generatetablefetch.columnNames", columnNames);
                    }
                    if (StringUtils.isNotBlank(whereClause)) {
                        sqlFlowFile = session.putAttribute(sqlFlowFile, "generatetablefetch.whereClause", whereClause);
                    }
                    if (StringUtils.isNotBlank(maxColumnNames)) {
                        sqlFlowFile = session.putAttribute(sqlFlowFile, "generatetablefetch.maxColumnNames", maxColumnNames);
                    }
                    sqlFlowFile = session.putAttribute(sqlFlowFile, "generatetablefetch.limit", String.valueOf(limit));
                    if (partitionSize != 0) {
                        sqlFlowFile = session.putAttribute(sqlFlowFile, "generatetablefetch.offset", String.valueOf(offset));
                    }
                    session.transfer(sqlFlowFile, REL_SUCCESS);
                }

                if (fileToProcess != null) {
                    session.remove(fileToProcess);
                }
            } catch (SQLException e) {
                if (fileToProcess != null) {
                    logger.error("Unable to execute SQL select query {} due to {}, routing {} to failure", new Object[]{selectQuery, e, fileToProcess});
                    fileToProcess = session.putAttribute(fileToProcess, "generatetablefetch.sql.error", e.getMessage());
                    session.transfer(fileToProcess, REL_FAILURE);

                } else {
                    logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
                    throw new ProcessException(e);
                }
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

    private String getColumnStateMaxValue(String tableName, Map<String, String> statePropertyMap, String colName) {
        final String fullyQualifiedStateKey = getStateKey(tableName, colName);
        String maxValue = statePropertyMap.get(fullyQualifiedStateKey);
        if (StringUtils.isEmpty(maxValue) && !isDynamicTableName) {
            // If the table name is static and the fully-qualified key was not found, try just the column name
            maxValue = statePropertyMap.get(getStateKey(null, colName));
        }

        return maxValue;
    }

    private Integer getColumnType(String tableName, String colName) {
        final String fullyQualifiedStateKey = getStateKey(tableName, colName);
        Integer type = columnTypeMap.get(fullyQualifiedStateKey);
        if (type == null && !isDynamicTableName) {
            // If the table name is static and the fully-qualified key was not found, try just the column name
            type = columnTypeMap.get(getStateKey(null, colName));
        }
        if (type == null) {
            // This shouldn't happen as we are populating columnTypeMap when the processor is scheduled or when the first maximum is observed
            throw new ProcessException("No column type cache found for: " + colName);
        }

        return type;
    }
}
