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
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.JdbcCommon;
import org.apache.nifi.util.LongHolder;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

@EventDriven
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("Execute provided SQL select query. Query result will be converted to Avro format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'querydbtable.row.count' indicates how many rows were selected.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttribute(attribute = "querydbtable.row.count")
public class QueryDatabaseTable extends AbstractSessionFactoryProcessor {

    public static final String RESULT_ROW_COUNT = "querydbtable.row.count";

    public static final String SQL_PREPROCESS_STRATEGY_NONE = "None";
    public static final String SQL_PREPROCESS_STRATEGY_ORACLE = "Oracle";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    private final Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the database table to be queried.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Columns to Return")
            .description("A comma-separated list of column names to be used in the query. If your database requires "
                    + "special treatment of the names (quoting, e.g.), each name should include such treatment. If no "
                    + "column names are supplied, all columns in the specified table will be returned.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_VALUE_COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Maximum-value Columns")
            .description("A comma-separated list of column names. The processor will keep track of the maximum value "
                    + "for each column that has been returned since the processor started running. This can be used to "
                    + "retrieve only those rows that have been added/updated since the last retrieval. Note that some "
                    + "JDBC types such as bit/boolean are not conducive to maintaining maximum value, so columns of these "
                    + "types should not be listed in this property, and will result in error(s) during processing.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor SQL_PREPROCESS_STRATEGY = new PropertyDescriptor.Builder()
            .name("SQL Pre-processing Strategy")
            .description("The strategy to employ when generating the SQL for querying the table. A strategy may include "
                    + "custom or database-specific code, such as the treatment of time/date formats.")
            .required(true)
            .allowableValues(SQL_PREPROCESS_STRATEGY_NONE, SQL_PREPROCESS_STRATEGY_ORACLE)
            .defaultValue("None")
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    private final List<PropertyDescriptor> propDescriptors;

    protected final Map<String, Integer> columnTypeMap = new HashMap<>();

    public QueryDatabaseTable() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(TABLE_NAME);
        pds.add(COLUMN_NAMES);
        pds.add(MAX_VALUE_COLUMN_NAMES);
        pds.add(QUERY_TIMEOUT);
        pds.add(SQL_PREPROCESS_STRATEGY);
        pds.add(FETCH_SIZE);
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
        // Try to fill the columnTypeMap with the types of the desired max-value columns
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).getValue();

        try (final Connection con = dbcpService.getConnection();
             final Statement st = con.createStatement()) {

            // Try a query that returns no rows, for the purposes of getting metadata about the columns. It is possible
            // to use DatabaseMetaData.getColumns(), but not all drivers support this, notably the schema-on-read
            // approach as in Apache Drill
            String query = getSelectFromClause(tableName, maxValueColumnNames).append(" WHERE 1 = 0").toString();
            ResultSet resultSet = st.executeQuery(query);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int numCols = resultSetMetaData.getColumnCount();
            if (numCols > 0) {
                columnTypeMap.clear();
                for (int i = 1; i <= numCols; i++) {
                    String colName = resultSetMetaData.getColumnName(i).toLowerCase();
                    int colType = resultSetMetaData.getColumnType(i);
                    columnTypeMap.put(colName, colType);
                }

            } else {
                throw new ProcessException("No columns found in table from those specified: " + maxValueColumnNames);
            }

        } catch (SQLException e) {
            throw new ProcessException("Unable to communicate with database in order to determine column types", e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();
        FlowFile fileToProcess = null;

        final ProcessorLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).getValue();
        final String preProcessStrategy = context.getProperty(SQL_PREPROCESS_STRATEGY).getValue();
        final Integer fetchSize = context.getProperty(FETCH_SIZE).asInteger();

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

        final String selectQuery = getQuery(tableName, columnNames, getColumns(maxValueColumnNames), stateMap, preProcessStrategy);
        final StopWatch stopWatch = new StopWatch(true);

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

            final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
            st.setQueryTimeout(queryTimeout); // timeout in seconds

            final LongHolder nrOfRows = new LongHolder(0L);

            fileToProcess = session.create();
            fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try {
                        logger.debug("Executing query {}", new Object[]{selectQuery});
                        final ResultSet resultSet = st.executeQuery(selectQuery);
                        // Max values will be updated in the state property map by the callback
                        final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(statePropertyMap, preProcessStrategy);
                        nrOfRows.set(JdbcCommon.convertToAvroStream(resultSet, out, tableName, maxValCollector));

                    } catch (final SQLException e) {
                        throw new ProcessException("Error during database query or conversion of records to Avro", e);
                    }
                }
            });

            if (nrOfRows.get() > 0) {
                // set attribute how many rows were selected
                fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, nrOfRows.get().toString());

                logger.info("{} contains {} Avro records; transferring to 'success'",
                        new Object[]{fileToProcess, nrOfRows.get()});
                String jdbcURL = "DBCPService";
                try {
                    DatabaseMetaData databaseMetaData = con.getMetaData();
                    if (databaseMetaData != null) {
                        jdbcURL = databaseMetaData.getURL();
                    }
                } catch (SQLException se) {
                    // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
                }
                session.getProvenanceReporter().receive(fileToProcess, jdbcURL, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(fileToProcess, REL_SUCCESS);
            } else {
                // If there were no rows returned, don't send the flowfile
                session.remove(fileToProcess);
                context.yield();
            }

        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
            if (fileToProcess != null) {
                session.remove(fileToProcess);
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

    protected List<String> getColumns(String commaSeparatedColumnList) {
        if (StringUtils.isEmpty(commaSeparatedColumnList)) {
            return Collections.emptyList();
        }
        final String[] columns = commaSeparatedColumnList.split(",");
        final List<String> columnList = new ArrayList<>(columns.length);
        for (String column : columns) {
            if (column != null) {
                String trimmedColumn = column.trim();
                if (!StringUtils.isEmpty(trimmedColumn)) {
                    columnList.add(trimmedColumn);
                }
            }
        }
        return columnList;
    }

    protected String getQuery(String tableName, String columnNames, List<String> maxValColumnNames,
                              StateMap stateMap, String preProcessStrategy) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        final StringBuilder query = new StringBuilder(getSelectFromClause(tableName, columnNames));

        // Check state map for last max values
        if (stateMap != null && stateMap.getVersion() != -1 && maxValColumnNames != null) {
            Map<String, String> stateProperties = stateMap.toMap();
            List<String> whereClauses = new ArrayList<>(maxValColumnNames.size());
            for (String colName : maxValColumnNames) {
                String maxValue = stateProperties.get(colName.toLowerCase());
                if (!StringUtils.isEmpty(maxValue)) {
                    Integer type = columnTypeMap.get(colName.toLowerCase());
                    if (type == null) {
                        // This shouldn't happen as we are populating columnTypeMap when the processor is scheduled.
                        throw new IllegalArgumentException("No column type found for: " + colName);
                    }
                    // Add a condition for the WHERE clause
                    whereClauses.add(colName + " > " + getLiteralByType(type, maxValue, preProcessStrategy));
                }
            }
            if (!whereClauses.isEmpty()) {
                query.append(" WHERE ");
                query.append(StringUtils.join(whereClauses, " AND "));
            }
        }

        return query.toString();
    }

    /**
     * Returns a basic SELECT ... FROM clause with the given column names and table name. If no column names are found,
     * the wildcard (*) is used to select all columns.
     *
     * @param tableName   The name of the table to select from
     * @param columnNames A comma-separated list of column names to select from the table
     * @return A SQL select statement representing a query of the given column names from the given table
     */
    protected StringBuilder getSelectFromClause(String tableName, String columnNames) {
        final StringBuilder query = new StringBuilder("SELECT ");
        if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
            query.append("*");
        } else {
            query.append(columnNames);
        }
        query.append(" FROM ");
        query.append(tableName);
        return query;
    }

    /**
     * Returns a SQL literal for the given value based on its type. For example, values of character type need to be enclosed
     * in single quotes, whereas values of numeric type should not be.
     *
     * @param type  The JDBC type for the desired literal
     * @param value The value to be converted to a SQL literal
     * @return A String representing the given value as a literal of the given type
     */
    protected String getLiteralByType(int type, String value, String preProcessStrategy) {
        // Format value based on column type. For example, strings and timestamps need to be quoted
        switch (type) {
            // For string-represented values, put in single quotes
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
            case DATE:
            case TIME:
                return "'" + value + "'";
            case TIMESTAMP:
                // Timestamp literals in Oracle need to be cast with TO_DATE
                if (SQL_PREPROCESS_STRATEGY_ORACLE.equals(preProcessStrategy)) {
                    return "to_date('" + value + "', 'yyyy-mm-dd HH24:MI:SS')";
                } else {
                    return "'" + value + "'";
                }
                // Else leave as is (numeric types, e.g.)
            default:
                return value;
        }
    }

    protected class MaxValueResultSetRowCollector implements JdbcCommon.ResultSetRowCallback {
        String preProcessStrategy;
        Map<String, String> newColMap;

        public MaxValueResultSetRowCollector(Map<String, String> stateMap, String preProcessStrategy) {
            this.preProcessStrategy = preProcessStrategy;
            newColMap = stateMap;
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
                        Integer type = columnTypeMap.get(colName);
                        // Skip any columns we're not keeping track of or whose value is null
                        if (type == null || resultSet.getObject(i) == null) {
                            continue;
                        }
                        String maxValueString = newColMap.get(colName);
                        switch (type) {
                            case CHAR:
                            case LONGNVARCHAR:
                            case LONGVARCHAR:
                            case NCHAR:
                            case NVARCHAR:
                            case VARCHAR:
                            case ROWID:
                                String colStringValue = resultSet.getString(i);
                                if (maxValueString == null || colStringValue.compareTo(maxValueString) > 0) {
                                    newColMap.put(colName, colStringValue);
                                }
                                break;

                            case INTEGER:
                            case SMALLINT:
                            case TINYINT:
                                Integer colIntValue = resultSet.getInt(i);
                                Integer maxIntValue = null;
                                if (maxValueString != null) {
                                    maxIntValue = Integer.valueOf(maxValueString);
                                }
                                if (maxIntValue == null || colIntValue > maxIntValue) {
                                    newColMap.put(colName, colIntValue.toString());
                                }
                                break;

                            case BIGINT:
                                Long colLongValue = resultSet.getLong(i);
                                Long maxLongValue = null;
                                if (maxValueString != null) {
                                    maxLongValue = Long.valueOf(maxValueString);
                                }
                                if (maxLongValue == null || colLongValue > maxLongValue) {
                                    newColMap.put(colName, colLongValue.toString());
                                }
                                break;

                            case FLOAT:
                            case REAL:
                            case DOUBLE:
                                Double colDoubleValue = resultSet.getDouble(i);
                                Double maxDoubleValue = null;
                                if (maxValueString != null) {
                                    maxDoubleValue = Double.valueOf(maxValueString);
                                }
                                if (maxDoubleValue == null || colDoubleValue > maxDoubleValue) {
                                    newColMap.put(colName, colDoubleValue.toString());
                                }
                                break;

                            case DECIMAL:
                            case NUMERIC:
                                BigDecimal colBigDecimalValue = resultSet.getBigDecimal(i);
                                BigDecimal maxBigDecimalValue = null;
                                if (maxValueString != null) {
                                    DecimalFormat df = new DecimalFormat();
                                    df.setParseBigDecimal(true);
                                    maxBigDecimalValue = (BigDecimal) df.parse(maxValueString);
                                }
                                if (maxBigDecimalValue == null || colBigDecimalValue.compareTo(maxBigDecimalValue) > 0) {
                                    newColMap.put(colName, colBigDecimalValue.toString());
                                }
                                break;

                            case DATE:
                                Date rawColDateValue = resultSet.getDate(i);
                                java.sql.Date colDateValue = new java.sql.Date(rawColDateValue.getTime());
                                java.sql.Date maxDateValue = null;
                                if (maxValueString != null) {
                                    maxDateValue = java.sql.Date.valueOf(maxValueString);
                                }
                                if (maxDateValue == null || colDateValue.after(maxDateValue)) {
                                    newColMap.put(colName, colDateValue.toString());
                                }
                                break;

                            case TIME:
                                Date rawColTimeValue = resultSet.getDate(i);
                                java.sql.Time colTimeValue = new java.sql.Time(rawColTimeValue.getTime());
                                java.sql.Time maxTimeValue = null;
                                if (maxValueString != null) {
                                    maxTimeValue = java.sql.Time.valueOf(maxValueString);
                                }
                                if (maxTimeValue == null || colTimeValue.after(maxTimeValue)) {
                                    newColMap.put(colName, colTimeValue.toString());
                                }
                                break;

                            case TIMESTAMP:
                                // Oracle timestamp queries must use literals in java.sql.Date format
                                if (SQL_PREPROCESS_STRATEGY_ORACLE.equals(preProcessStrategy)) {
                                    Date rawColOracleTimestampValue = resultSet.getDate(i);
                                    java.sql.Date oracleTimestampValue = new java.sql.Date(rawColOracleTimestampValue.getTime());
                                    java.sql.Date maxOracleTimestampValue = null;
                                    if (maxValueString != null) {
                                        maxOracleTimestampValue = java.sql.Date.valueOf(maxValueString);
                                    }
                                    if (maxOracleTimestampValue == null || oracleTimestampValue.after(maxOracleTimestampValue)) {
                                        newColMap.put(colName, oracleTimestampValue.toString());
                                    }
                                } else {
                                    Timestamp rawColTimestampValue = resultSet.getTimestamp(i);
                                    java.sql.Timestamp colTimestampValue = new java.sql.Timestamp(rawColTimestampValue.getTime());
                                    java.sql.Timestamp maxTimestampValue = null;
                                    if (maxValueString != null) {
                                        maxTimestampValue = java.sql.Timestamp.valueOf(maxValueString);
                                    }
                                    if (maxTimestampValue == null || colTimestampValue.after(maxTimestampValue)) {
                                        newColMap.put(colName, colTimestampValue.toString());
                                    }
                                }
                                break;

                            case BIT:
                            case BOOLEAN:
                            case BINARY:
                            case VARBINARY:
                            case LONGVARBINARY:
                            case ARRAY:
                            case BLOB:
                            case CLOB:
                            default:
                                throw new IOException("Type " + meta.getColumnTypeName(i) + " is not valid for maintaining maximum value");
                        }
                    }
                }
            } catch (ParseException | SQLException e) {
                throw new IOException(e);
            }

        }
    }
}
