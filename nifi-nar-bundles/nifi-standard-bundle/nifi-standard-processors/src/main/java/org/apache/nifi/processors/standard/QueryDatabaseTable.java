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
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.processors.standard.util.JdbcCommon;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.apache.nifi.processors.standard.util.JdbcCommon.DEFAULT_PRECISION;
import static org.apache.nifi.processors.standard.util.JdbcCommon.DEFAULT_SCALE;
import static org.apache.nifi.processors.standard.util.JdbcCommon.NORMALIZE_NAMES_FOR_AVRO;
import static org.apache.nifi.processors.standard.util.JdbcCommon.USE_AVRO_LOGICAL_TYPES;


@EventDriven
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database"})
@SeeAlso({GenerateTableFetch.class, ExecuteSQL.class})
@CapabilityDescription("Generates and executes a SQL select query to fetch all rows whose values in the specified Maximum Value column(s) are larger than the "
        + "previously-seen maxima. Query result will be converted to Avro format. Expression Language is supported for several properties, but no incoming "
        + "connections are permitted. The Variable Registry may be used to provide values for any property containing Expression Language. If it is desired to "
        + "leverage flow file attributes to perform these queries, the GenerateTableFetch and/or ExecuteSQL processors can be used for this purpose. "
        + "Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer or cron expression, using the standard scheduling methods. This processor is intended to be run on the Primary Node only. FlowFile attribute "
        + "'querydbtable.row.count' indicates how many rows were selected.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttributes({
        @WritesAttribute(attribute = "tablename", description="Name of the table being queried"),
        @WritesAttribute(attribute = "querydbtable.row.count", description="The number of rows selected by the query"),
        @WritesAttribute(attribute="fragment.identifier", description="If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute="fragment.count", description="If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet."),
        @WritesAttribute(attribute="fragment.index", description="If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "maxvalue.*", description = "Each attribute contains the observed maximum value of a specified 'Maximum-value Column'. The "
                + "suffix of the attribute is the name of the column")})
@DynamicProperty(name = "Initial Max Value", value = "Attribute Expression Language", supportsExpressionLanguage = false, description = "Specifies an initial "
        + "max value for max value columns. Properties should be added in the format `initial.maxvalue.{max_value_column}`.")
public class QueryDatabaseTable extends AbstractDatabaseFetchProcessor {

    public static final String RESULT_TABLENAME = "tablename";
    public static final String RESULT_ROW_COUNT = "querydbtable.row.count";

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("qdbt-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. " +
                    "This will allow you to break up very large result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor MAX_FRAGMENTS = new PropertyDescriptor.Builder()
            .name("qdbt-max-frags")
            .displayName("Maximum Number of Fragments")
            .description("The maximum number of fragments. If the value specified is zero, then all fragments are returned. " +
                    "This prevents OutOfMemoryError when this processor ingests huge table.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public QueryDatabaseTable() {
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
        pds.add(FETCH_SIZE);
        pds.add(MAX_ROWS_PER_FLOW_FILE);
        pds.add(MAX_FRAGMENTS);
        pds.add(NORMALIZE_NAMES_FOR_AVRO);
        pds.add(USE_AVRO_LOGICAL_TYPES);
        pds.add(DEFAULT_PRECISION);
        pds.add(DEFAULT_SCALE);
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

    @OnScheduled
    public void setup(final ProcessContext context) {
        maxValueProperties = getDefaultMaxValueProperties(context.getProperties());
        super.setup(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final String customWhereClause = context.getProperty(WHERE_CLAUSE).evaluateAttributeExpressions().getValue();
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions().asInteger()
                : 0;
        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                .recordName(tableName)
                .maxRows(maxRowsPerFlowFile)
                .convertNames(context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean())
                .useLogicalTypes(context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean())
                .defaultPrecision(context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions().asInteger())
                .defaultScale(context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions().asInteger())
                .build();

        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }
        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        //If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
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

        List<String> maxValueColumnNameList = StringUtils.isEmpty(maxValueColumnNames)
                ? null
                : Arrays.asList(maxValueColumnNames.split("\\s*,\\s*"));
        final String selectQuery = getQuery(dbAdapter, tableName, columnNames, maxValueColumnNameList, customWhereClause, statePropertyMap);
        final StopWatch stopWatch = new StopWatch(true);
        final String fragmentIdentifier = UUID.randomUUID().toString();

        try (final Connection con = dbcpService.getConnection();
             final Statement st = con.createStatement()) {

            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }

            String jdbcURL = "DBCPService";
            try {
                DatabaseMetaData databaseMetaData = con.getMetaData();
                if (databaseMetaData != null) {
                    jdbcURL = databaseMetaData.getURL();
                }
            } catch (SQLException se) {
                // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
            }

            final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();
            st.setQueryTimeout(queryTimeout); // timeout in seconds
            try {
                logger.debug("Executing query {}", new Object[]{selectQuery});
                final ResultSet resultSet = st.executeQuery(selectQuery);
                int fragmentIndex=0;
                while(true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    FlowFile fileToProcess = session.create();
                    try {
                        fileToProcess = session.write(fileToProcess, out -> {
                            // Max values will be updated in the state property map by the callback
                            final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap, dbAdapter);
                            try {
                                nrOfRows.set(JdbcCommon.convertToAvroStream(resultSet, out, options, maxValCollector));
                            } catch (SQLException | RuntimeException e) {
                                throw new ProcessException("Error during database query or conversion of records to Avro.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(fileToProcess);
                        throw e;
                    }

                    if (nrOfRows.get() > 0) {
                        // set attribute how many rows were selected
                        fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                        fileToProcess = session.putAttribute(fileToProcess, RESULT_TABLENAME, tableName);
                        fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);
                        if(maxRowsPerFlowFile > 0) {
                            fileToProcess = session.putAttribute(fileToProcess, "fragment.identifier", fragmentIdentifier);
                            fileToProcess = session.putAttribute(fileToProcess, "fragment.index", String.valueOf(fragmentIndex));
                        }

                        logger.info("{} contains {} Avro records; transferring to 'success'",
                                new Object[]{fileToProcess, nrOfRows.get()});

                        session.getProvenanceReporter().receive(fileToProcess, jdbcURL, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        resultSetFlowFiles.add(fileToProcess);
                    } else {
                        // If there were no rows returned, don't send the flowfile
                        session.remove(fileToProcess);
                        context.yield();
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }
                }

                for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                    // Add maximum values as attributes
                    for (Map.Entry<String, String> entry : statePropertyMap.entrySet()) {
                        // Get just the column name from the key
                        String key = entry.getKey();
                        String colName = key.substring(key.lastIndexOf(NAMESPACE_DELIMITER) + NAMESPACE_DELIMITER.length());
                        resultSetFlowFiles.set(i, session.putAttribute(resultSetFlowFiles.get(i), "maxvalue." + colName, entry.getValue()));
                    }

                    //set count on all FlowFiles
                    if(maxRowsPerFlowFile > 0) {
                        resultSetFlowFiles.set(i,
                                session.putAttribute(resultSetFlowFiles.get(i), "fragment.count", Integer.toString(fragmentIndex)));
                    }
                }

            } catch (final SQLException e) {
                throw e;
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);

        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
            if (!resultSetFlowFiles.isEmpty()) {
                session.remove(resultSetFlowFiles);
            }
            context.yield();
        } finally {
            session.commit();
            try {
                // Update the state
                stateManager.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", new Object[]{this, ioe});
            }
        }
    }

    protected String getQuery(DatabaseAdapter dbAdapter, String tableName, String columnNames, List<String> maxValColumnNames,
                              String customWhereClause, Map<String, String> stateMap) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        final StringBuilder query = new StringBuilder(dbAdapter.getSelectStatement(tableName, columnNames, null, null, null, null));

        List<String> whereClauses = new ArrayList<>();
        // Check state map for last max values
        if (stateMap != null && !stateMap.isEmpty() && maxValColumnNames != null) {
            IntStream.range(0, maxValColumnNames.size()).forEach((index) -> {
                String colName = maxValColumnNames.get(index);
                String maxValueKey = getStateKey(tableName, colName);
                String maxValue = stateMap.get(maxValueKey);
                if (StringUtils.isEmpty(maxValue)) {
                    // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                    // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                    // maximum value is observed, it will be stored under the fully-qualified key from then on.
                    maxValue = stateMap.get(colName.toLowerCase());
                }
                if (!StringUtils.isEmpty(maxValue)) {
                    Integer type = columnTypeMap.get(maxValueKey);
                    if (type == null) {
                        // This shouldn't happen as we are populating columnTypeMap when the processor is scheduled.
                        throw new IllegalArgumentException("No column type found for: " + colName);
                    }
                    // Add a condition for the WHERE clause
                    whereClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue, dbAdapter.getName()));
                }
            });
        }

        if (customWhereClause != null) {
            whereClauses.add("(" + customWhereClause + ")");
        }

        if (!whereClauses.isEmpty()) {
            query.append(" WHERE ");
            query.append(StringUtils.join(whereClauses, " AND "));
        }

        return query.toString();
    }

    protected class MaxValueResultSetRowCollector implements JdbcCommon.ResultSetRowCallback {
        DatabaseAdapter dbAdapter;
        Map<String, String> newColMap;
        String tableName;

        public MaxValueResultSetRowCollector(String tableName, Map<String, String> stateMap, DatabaseAdapter dbAdapter) {
            this.dbAdapter = dbAdapter;
            newColMap = stateMap;
            this.tableName = tableName;
        }

        @Override
        public void processRow(ResultSet resultSet) throws IOException {
            if (resultSet == null) {
                return;
            }
            try {
                // Iterate over the row, check-and-set max values
                final ResultSetMetaData meta = resultSet.getMetaData();
                final int nrOfColumns = meta.getColumnCount();
                if (nrOfColumns > 0) {
                    for (int i = 1; i <= nrOfColumns; i++) {
                        String colName = meta.getColumnName(i).toLowerCase();
                        String fullyQualifiedMaxValueKey = getStateKey(tableName, colName);
                        Integer type = columnTypeMap.get(fullyQualifiedMaxValueKey);
                        // Skip any columns we're not keeping track of or whose value is null
                        if (type == null || resultSet.getObject(i) == null) {
                            continue;
                        }
                        String maxValueString = newColMap.get(fullyQualifiedMaxValueKey);
                        // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                        // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                        // maximum value is observed, it will be stored under the fully-qualified key from then on.
                        if (StringUtils.isEmpty(maxValueString)) {
                            maxValueString = newColMap.get(colName);
                        }
                        String newMaxValueString = getMaxValueFromRow(resultSet, i, type, maxValueString, dbAdapter.getName());
                        if (newMaxValueString != null) {
                            newColMap.put(fullyQualifiedMaxValueKey, newMaxValueString);
                        }
                    }
                }
            } catch (ParseException | SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
