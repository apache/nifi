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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.FormatUtils;
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
import java.util.regex.Pattern;

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

    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_IDLE} in Commons-DBCP 2.5.0
     */
    private static final String DEFAULT_MIN_IDLE = "0";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MAX_IDLE} in Commons-DBCP 2.5.0
     */
    private static final String DEFAULT_MAX_IDLE = "8";
    /**
     * Copied from private variable {@link BasicDataSource.maxConnLifetimeMillis} in Commons-DBCP 2.5.0
     */
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS} in Commons-DBCP 2.5.0
     */
    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.5.0
     * and converted from 1800000L to "1800000 millis" to "30 mins"
     */
    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.5.0
     */
    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    private static final Validator CUSTOM_TIME_PERIOD_VALIDATOR = new Validator() {
        private final Pattern TIME_DURATION_PATTERN = Pattern.compile(FormatUtils.TIME_DURATION_REGEX);

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (input == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("Time Period cannot be null").build();
            }
            if (TIME_DURATION_PATTERN.matcher(input.toLowerCase()).matches() || input.equals("-1")) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } else {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Must be of format <duration> <TimeUnit> where <duration> is a "
                                + "non-negative integer and TimeUnit is a supported Time Unit, such "
                                + "as: nanos, millis, secs, mins, hrs, days")
                        .build();
            }
        }
    };

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
        .addValidator(CUSTOM_TIME_PERIOD_VALIDATOR)
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

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .displayName("Minimum Idle Connections")
            .name("dbcp-min-idle-conns")
            .description("The minimum number of connections that can remain idle in the pool, without extra ones being " +
                    "created, or zero to create none.")
            .defaultValue(DEFAULT_MIN_IDLE)
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
            .displayName("Max Idle Connections")
            .name("dbcp-max-idle-conns")
            .description("The maximum number of connections that can remain idle in the pool, without extra ones being " +
                    "released, or negative for no limit.")
            .defaultValue(DEFAULT_MAX_IDLE)
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .displayName("Max Connection Lifetime")
            .name("dbcp-max-conn-lifetime")
            .description("The maximum lifetime in milliseconds of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
            .displayName("Time Between Eviction Runs")
            .name("dbcp-time-between-eviction-runs")
            .description("The number of milliseconds to sleep between runs of the idle connection evictor thread. When " +
                    "non-positive, no idle connection evictor thread will be run.")
            .defaultValue(DEFAULT_EVICTION_RUN_PERIOD)
            .required(false)
            .addValidator(CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Minimum Evictable Idle Time")
            .name("dbcp-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
            .defaultValue(DEFAULT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Soft Minimum Evictable Idle Time")
            .name("dbcp-soft-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for " +
                    "eviction by the idle connection evictor, with the extra condition that at least a minimum number of" +
                    " idle connections remain in the pool. When the not-soft version of this option is set to a positive" +
                    " value, it is examined first by the idle connection evictor: when idle connections are visited by " +
                    "the evictor, idle time is first compared against it (without considering the number of idle " +
                    "connections in the pool) and then against this soft option, including the minimum idle connections " +
                    "constraint.")
            .defaultValue(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(CUSTOM_TIME_PERIOD_VALIDATOR)
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
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);

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
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME));
        final Integer minIdle = context.getProperty(MIN_IDLE).asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME));
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD));
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME));
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME));

        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(drv);

        // Optional driver URL, when exist, this URL will be used to locate driver jar file location
        final String urlString = context.getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions().getValue();
        dataSource.setDriverClassLoader(getDriverClassLoader(urlString, drv));

        final String dburl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();

        dataSource.setMaxWaitMillis(maxWaitMillis);
        dataSource.setMaxTotal(maxTotal);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis);
        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);

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

    private Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
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

    BasicDataSource getDataSource() {
        return dataSource;
    }
}
