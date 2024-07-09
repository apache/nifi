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

package org.apache.nifi.sql;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.FunctionContext;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * A simple abstraction for creating a database facade over data.
 * </p>
 *
 * <p>
 * While this abstraction does not provide all of the power that apache Calcite provides,
 * it does provide the capabilities that are necessary for the uses cases where NiFi has made use of Calcite.
 * This abstraction, however, greatly simplifies the API so that we can easily expose a SQL interface to data.
 * </p>
 *
 * <p>
 * The typical usage pattern for this class is as follows:
 * </p>
 *
 * <pre>
 * <code>
 *     try (final CalciteDatabase database = new CalciteDatabase()) {
 *         final ResettableDataSource dataSource = getDataSource();
 *         final NiFiTable firstTable = new NiFiTable("MY_TABLE", dataSource, getLogger());
 *         database.addTable(firstTable);
 *
 *         final ResettableDataSource secondDataSource = getDataSource();
 *         final NiFiTable secondTable = new NiFiTable("YOUR_TABLE", secondDataSource, getLogger());
 *         database.addTable(secondTable);
 *
 *         try (final PreparedStatement stmt = database.getConnection().prepareStatement("SELECT * FROM MY_TABLE")) {
 *             ...
 *         }
 *     }
 * </code>
 * </pre>
 */
public class CalciteDatabase implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(CalciteDatabase.class);

    private final CalciteConnection connection;
    private final Map<String, NiFiTable> tables = new HashMap<>();

    static {
        try {
            DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
        } catch (final Exception e) {
            throw new RuntimeException("Could not initialize Calcite JDBC Driver");
        }
    }

    /**
     * Creates a new database using the default connection properties
     *
     * @throws SQLException if unable to create the database/connection
     */
    public CalciteDatabase() throws SQLException {
        this(null);
    }

    /**
     * Creates a new database with the given calcite connection properties
     *
     * @param calciteConnectionProperties the connection properties to provide to calcite
     * @throws SQLException if unable to create the database/connection
     */
    public CalciteDatabase(final Properties calciteConnectionProperties) throws SQLException {
        this.connection = createConnection(calciteConnectionProperties);
    }

    /**
     * Adds the given table to the database
     *
     * @param table the table to add
     */
    public void addTable(final NiFiTable table) {
        if (tables.containsKey(table.getName())) {
            throw new IllegalStateException("Database already contains a table named " + table.getName());
        }

        connection.getRootSchema().add(table.getName(), table.createCalciteTable());
        tables.put(table.getName(), table);
    }

    /**
     * Retrieves the table with the given name
     *
     * @param tableName the name of the table
     * @return the NiFiTable with the given name, or <code>null</code> if no table exists with the given name
     */
    public NiFiTable getTable(final String tableName) {
        return tables.get(tableName);
    }

    /**
     * @return all NiFiTables in the database
     */
    public Collection<NiFiTable> getTables() {
        return Collections.unmodifiableCollection(tables.values());
    }

    /**
     * Removes the table with the given name from the database
     *
     * @param tableName the name of the table
     * @return the NiFiTable that was removed, or <code>null</code> if no table was removed
     */
    public NiFiTable removeTable(final String tableName) {
        final NiFiTable table = tables.remove(tableName);
        if (table == null) {
            return null;
        }

        connection.getRootSchema().removeTable(tableName);
        return table;
    }

    /**
     * Adds a user-defined function (UDF) to the database with the given name.
     * Note that Calcite requires that the Class have a default, zero-arg constructor,
     * or a constructor that takes exactly one argument of type {@link FunctionContext}.
     *
     * @param functionName  the name of the function
     * @param functionClass the class that implements the function
     * @param methodName    the name of the method that implements the function
     */
    public void addUserDefinedFunction(final String functionName, final Class<?> functionClass, final String methodName) {
        connection.getRootSchema().add(functionName, ScalarFunctionImpl.create(functionClass, methodName));
    }

    /**
     * Adds a user-defined function (UDF) to the database with the given name.
     * Note that Calcite requires that the Class that the given method belongs to have a default, zero-arg constructor,
     * or a constructor that takes exactly one argument of type {@link FunctionContext}.
     *
     * @param functionName the name of the function
     * @param method       the method to invoke in order to call the function
     */
    public void addUserDefinedFunction(final String functionName, final Method method) {
        connection.getRootSchema().add(functionName, ScalarFunctionImpl.create(method));
    }

    /**
     * The database connection that can be used for creating Statements, etc.
     *
     * @return the database connection
     */
    public Connection getConnection() {
        return connection;
    }

    private CalciteConnection createConnection(final Properties properties) throws SQLException {
        final Properties calciteProperties;
        if (properties == null) {
            calciteProperties = new Properties();
            calciteProperties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());
        } else {
            calciteProperties = properties;
        }

        // If not explicitly set, default timezone to UTC. We ensure that when we provide timestamps, we convert them to UTC. We don't want
        // Calcite trying to convert them again.
        calciteProperties.putIfAbsent("timeZone", "UTC");

        final Connection sqlConnection = DriverManager.getConnection("jdbc:calcite:", calciteProperties);
        final CalciteConnection connection = sqlConnection.unwrap(CalciteConnection.class);
        connection.getRootSchema().setCacheEnabled(false);

        return connection;
    }

    @Override
    public void close() throws IOException {
        for (final NiFiTable table : getTables()) {
            try {
                table.close();
            } catch (final Exception e) {
                logger.warn("Encountered failure when attempting to close {}", table, e);
            }
        }

        try {
            connection.close();
        } catch (final SQLException e) {
            throw new IOException(e);
        }
    }
}
