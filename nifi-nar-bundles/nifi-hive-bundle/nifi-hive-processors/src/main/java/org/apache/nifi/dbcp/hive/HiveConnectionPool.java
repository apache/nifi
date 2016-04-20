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
package org.apache.nifi.dbcp.hive;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementation for Database Connection Pooling Service used for Apache Hive connections. Apache DBCP is used for connection pooling functionality.
 */
@Tags({"hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service for Apache Hive. Connections can be asked from pool and returned after usage.")
public class HiveConnectionPool extends AbstractControllerService implements HiveDBCPService {

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
                    + " The exact syntax of a database connection URL is specified by the Hive documentation. For example, the server principal is often included "
                    + "as a connection parameter when connecting to a secure Hive server.")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder().name("Hive Configuration Resources")
            .description("A file or comma separated list of files which contains the Hive configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration.")
            .required(false).addValidator(StandardValidators.createMultipleFilesExistValidator()).build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("Database User")
            .description("Database user name")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for the database user")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections) "
                    + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Total Connections")
            .description("The maximum number of active connections that can be allocated from this pool at the same time, "
                    + " or negative for no limit.")
            .defaultValue("8")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    private static List<PropertyDescriptor> properties;

    private volatile BasicDataSource dataSource;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(HIVE_CONFIGURATION_RESOURCES);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        properties = Collections.unmodifiableList(props);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link BasicDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        final String drv = HiveDriver.class.getName();
        final String user = context.getProperty(DB_USER).getValue();
        final String passw = context.getProperty(DB_PASSWORD).getValue();
        final Long maxWaitMillis = context.getProperty(MAX_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).asInteger();

        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(drv);

        final String dburl = context.getProperty(DATABASE_URL).getValue();

        dataSource.setMaxWait(maxWaitMillis);
        dataSource.setMaxActive(maxTotal);

        dataSource.setUrl(dburl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);
    }

    /**
     * Shutdown pool, close all open connections.
     */
    @OnDisabled
    public void shutdown() {
        try {
            dataSource.close();
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            final Connection con = dataSource.getConnection();
            return con;
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public String toString() {
        return "HiveConnectionPool[id=" + getIdentifier() + "]";
    }



}
