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
package org.apache.nifi.dbcp;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of for Database Connection Pooling Service. Apache DBCP is used for connection pooling functionality.
 *
 */
@Tags({ "dbcp", "jdbc", "database", "connection", "pooling", "store" })
@CapabilityDescription("Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage.")
@DynamicProperty(name = "JDBC property name", value = "JDBC property value", expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Specifies a property name and value to be set on the JDBC connection(s). "
                + "If Expression Language is used, evaluation will be performed upon the controller service being enabled. "
                + "Note that no flow file input (attributes, e.g.) is available for use in Expression Language constructs for these properties.")
public class DBCPConnectionPool extends AbstractControllerService implements DBCPService {

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
        .name("Database Connection URL")
        .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
            + " The exact syntax of a database connection URL is specified by your DBMS.")
        .defaultValue(null)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
        .name("Database Driver Class Name")
        .description("Database driver class name")
        .defaultValue(null)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
        .name("database-driver-locations")
        .displayName("Database Driver Location(s)")
        .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). For example '/var/tmp/mariadb-java-client-1.1.7.jar'")
        .defaultValue(null)
        .required(false)
        .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
        .name("Database User")
        .description("Database user name")
        .defaultValue(null)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .description("The password for the database user")
        .defaultValue(null)
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
        .name("Validation-query")
        .displayName("Validation query")
        .description("Validation query used to validate connections before returning them. "
            + "When connection is invalid, it get's dropped and new valid connection will be returned. "
            + "Note!! Using validation might have some performance penalty.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(DB_DRIVERNAME);
        props.add(DB_DRIVER_LOCATION);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);

        properties = Collections.unmodifiableList(props);
    }

    private volatile BasicDataSource dataSource;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link BasicDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     *
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        final String drv = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = context.getProperty(MAX_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();

        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(drv);

        // Optional driver URL, when exist, this URL will be used to locate driver jar file location
        final String urlString = context.getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions().getValue();
        dataSource.setDriverClassLoader(getDriverClassLoader(urlString, drv));

        final String dburl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();

        dataSource.setMaxWait(maxWaitMillis);
        dataSource.setMaxActive(maxTotal);

        if (validationQuery!=null && !validationQuery.isEmpty()) {
            dataSource.setValidationQuery(validationQuery);
            dataSource.setTestOnBorrow(true);
        }

        dataSource.setUrl(dburl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);

        context.getProperties().keySet().stream().filter(PropertyDescriptor::isDynamic)
                .forEach((dynamicPropDescriptor) -> dataSource.addConnectionProperty(dynamicPropDescriptor.getName(),
                        context.getProperty(dynamicPropDescriptor).evaluateAttributeExpressions().getValue()));

    }

    /**
     * using Thread.currentThread().getContextClassLoader(); will ensure that you are using the ClassLoader for you NAR.
     *
     * @throws InitializationException
     *             if there is a problem obtaining the ClassLoader
     */
    protected ClassLoader getDriverClassLoader(String locationString, String drvName) throws InitializationException {
        if (locationString != null && locationString.length() > 0) {
            try {
                // Split and trim the entries
                final ClassLoader classLoader = ClassLoaderUtils.getCustomClassLoader(
                        locationString,
                        this.getClass().getClassLoader(),
                        (dir, name) -> name != null && name.endsWith(".jar")
                );

                // Workaround which allows to use URLClassLoader for JDBC driver loading.
                // (Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.)
                final Class<?> clazz = Class.forName(drvName, true, classLoader);
                if (clazz == null) {
                    throw new InitializationException("Can't load Database Driver " + drvName);
                }
                final Driver driver = (Driver) clazz.newInstance();
                DriverManager.registerDriver(new DriverShim(driver));

                return classLoader;
            } catch (final MalformedURLException e) {
                throw new InitializationException("Invalid Database Driver Jar Url", e);
            } catch (final Exception e) {
                throw new InitializationException("Can't load Database Driver", e);
            }
        } else {
            // That will ensure that you are using the ClassLoader for you NAR.
            return Thread.currentThread().getContextClassLoader();
        }
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
        return "DBCPConnectionPool[id=" + getIdentifier() + "]";
    }

}
