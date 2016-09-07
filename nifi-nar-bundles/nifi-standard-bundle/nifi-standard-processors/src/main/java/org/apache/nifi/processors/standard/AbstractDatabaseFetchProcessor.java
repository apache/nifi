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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CLOB;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/**
 * A base class for common code shared by processors that fetch RDBMS data.
 */
public abstract class AbstractDatabaseFetchProcessor extends AbstractSessionFactoryProcessor {

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    protected Set<Relationship> relationships;

    // Properties
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
                    + "for each column that has been returned since the processor started running. Using multiple columns implies an order "
                    + "to the column list, and each column's values are expected to increase more slowly than the previous columns' values. Thus, "
                    + "using multiple columns implies a hierarchical structure of columns, which is usually used for partitioning tables. This processor "
                    + "can be used to retrieve only those rows that have been added/updated since the last retrieval. Note that some "
                    + "JDBC types such as bit/boolean are not conducive to maintaining maximum value, so columns of these "
                    + "types should not be listed in this property, and will result in error(s) during processing. If no columns "
                    + "are provided, all rows from the table will be considered, which could have a performance impact.")
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

    public static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
            .name("dbf-normalize")
            .displayName("Normalize Table/Column Names")
            .description("Whether to change non-Avro-compatible characters in column names to Avro-compatible characters. For example, colons and periods "
                    + "will be changed to underscores in order to build a valid Avro record.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    protected List<PropertyDescriptor> propDescriptors;

    public static final PropertyDescriptor DB_TYPE;

    protected final static Map<String, DatabaseAdapter> dbAdapters = new HashMap<>();
    protected final Map<String, Integer> columnTypeMap = new HashMap<>();

    private static SimpleDateFormat TIME_TYPE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

    static {
        // Load the DatabaseAdapters
        ServiceLoader<DatabaseAdapter> dbAdapterLoader = ServiceLoader.load(DatabaseAdapter.class);
        dbAdapterLoader.forEach(it -> dbAdapters.put(it.getName(), it));

        DB_TYPE = new PropertyDescriptor.Builder()
                .name("db-fetch-db-type")
                .displayName("Database Type")
                .description("The type/flavor of database, used for generating database-specific code. In many cases the Generic type "
                        + "should suffice, but some databases (such as Oracle) require custom SQL clauses. ")
                .allowableValues(dbAdapters.keySet())
                .defaultValue(dbAdapters.values().stream().findFirst().get().getName())
                .required(true)
                .build();
    }

    public void setup(final ProcessContext context) {
        // Try to fill the columnTypeMap with the types of the desired max-value columns
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).getValue();
        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());

        try (final Connection con = dbcpService.getConnection();
             final Statement st = con.createStatement()) {

            // Try a query that returns no rows, for the purposes of getting metadata about the columns. It is possible
            // to use DatabaseMetaData.getColumns(), but not all drivers support this, notably the schema-on-read
            // approach as in Apache Drill
            String query = dbAdapter.getSelectStatement(tableName, maxValueColumnNames, "1 = 0", null, null, null);
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

    protected static String getMaxValueFromRow(ResultSet resultSet,
                                               int columnIndex,
                                               Integer type,
                                               String maxValueString,
                                               String databaseType)
            throws ParseException, IOException, SQLException {

        // Skip any columns we're not keeping track of or whose value is null
        if (type == null || resultSet.getObject(columnIndex) == null) {
            return null;
        }

        switch (type) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
                String colStringValue = resultSet.getString(columnIndex);
                if (maxValueString == null || colStringValue.compareTo(maxValueString) > 0) {
                    return colStringValue;
                }
                break;

            case INTEGER:
            case SMALLINT:
            case TINYINT:
                Integer colIntValue = resultSet.getInt(columnIndex);
                Integer maxIntValue = null;
                if (maxValueString != null) {
                    maxIntValue = Integer.valueOf(maxValueString);
                }
                if (maxIntValue == null || colIntValue > maxIntValue) {
                    return colIntValue.toString();
                }
                break;

            case BIGINT:
                Long colLongValue = resultSet.getLong(columnIndex);
                Long maxLongValue = null;
                if (maxValueString != null) {
                    maxLongValue = Long.valueOf(maxValueString);
                }
                if (maxLongValue == null || colLongValue > maxLongValue) {
                    return colLongValue.toString();
                }
                break;

            case FLOAT:
            case REAL:
            case DOUBLE:
                Double colDoubleValue = resultSet.getDouble(columnIndex);
                Double maxDoubleValue = null;
                if (maxValueString != null) {
                    maxDoubleValue = Double.valueOf(maxValueString);
                }
                if (maxDoubleValue == null || colDoubleValue > maxDoubleValue) {
                    return colDoubleValue.toString();
                }
                break;

            case DECIMAL:
            case NUMERIC:
                BigDecimal colBigDecimalValue = resultSet.getBigDecimal(columnIndex);
                BigDecimal maxBigDecimalValue = null;
                if (maxValueString != null) {
                    DecimalFormat df = new DecimalFormat();
                    df.setParseBigDecimal(true);
                    maxBigDecimalValue = (BigDecimal) df.parse(maxValueString);
                }
                if (maxBigDecimalValue == null || colBigDecimalValue.compareTo(maxBigDecimalValue) > 0) {
                    return colBigDecimalValue.toString();
                }
                break;

            case DATE:
                Date rawColDateValue = resultSet.getDate(columnIndex);
                java.sql.Date colDateValue = new java.sql.Date(rawColDateValue.getTime());
                java.sql.Date maxDateValue = null;
                if (maxValueString != null) {
                    maxDateValue = java.sql.Date.valueOf(maxValueString);
                }
                if (maxDateValue == null || colDateValue.after(maxDateValue)) {
                    return colDateValue.toString();
                }
                break;

            case TIME:
                // Compare milliseconds-since-epoch. Need getTimestamp() instead of getTime() since some databases
                // don't return milliseconds in the Time returned by getTime().
                Date colTimeValue = new Date(resultSet.getTimestamp(columnIndex).getTime());
                Date maxTimeValue = null;
                if (maxValueString != null) {
                    try {
                        maxTimeValue = TIME_TYPE_FORMAT.parse(maxValueString);
                    } catch (ParseException pe) {
                        // Shouldn't happen, but just in case, leave the value as null so the new value will be stored
                    }
                }
                if (maxTimeValue == null || colTimeValue.after(maxTimeValue)) {
                    return TIME_TYPE_FORMAT.format(colTimeValue);
                }
                break;

            case TIMESTAMP:
                // Oracle timestamp queries must use literals in java.sql.Date format
                if ("Oracle".equals(databaseType)) {
                    Date rawColOracleTimestampValue = resultSet.getDate(columnIndex);
                    java.sql.Date oracleTimestampValue = new java.sql.Date(rawColOracleTimestampValue.getTime());
                    java.sql.Date maxOracleTimestampValue = null;
                    if (maxValueString != null) {
                        maxOracleTimestampValue = java.sql.Date.valueOf(maxValueString);
                    }
                    if (maxOracleTimestampValue == null || oracleTimestampValue.after(maxOracleTimestampValue)) {
                        return oracleTimestampValue.toString();
                    }
                } else {
                    Timestamp colTimestampValue = resultSet.getTimestamp(columnIndex);
                    java.sql.Timestamp maxTimestampValue = null;
                    if (maxValueString != null) {
                        maxTimestampValue = java.sql.Timestamp.valueOf(maxValueString);
                    }
                    if (maxTimestampValue == null || colTimestampValue.after(maxTimestampValue)) {
                        return colTimestampValue.toString();
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
                throw new IOException("Type for column " + columnIndex + " is not valid for maintaining maximum value");
        }
        return null;
    }

    /**
     * Returns a SQL literal for the given value based on its type. For example, values of character type need to be enclosed
     * in single quotes, whereas values of numeric type should not be.
     *
     * @param type  The JDBC type for the desired literal
     * @param value The value to be converted to a SQL literal
     * @return A String representing the given value as a literal of the given type
     */
    protected static String getLiteralByType(int type, String value, String databaseType) {
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
                if ("Oracle".equals(databaseType)) {
                    return "to_date('" + value + "', 'yyyy-mm-dd HH24:MI:SS')";
                } else {
                    return "'" + value + "'";
                }
                // Else leave as is (numeric types, e.g.)
            default:
                return value;
        }
    }
}
