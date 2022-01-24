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
package org.apache.nifi.snowflake.service;

import net.snowflake.client.jdbc.SnowflakeDriver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.dbcp.DBCPValidator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of Database Connection Pooling Service for Snowflake.
 * Apache DBCP is used for connection pooling functionality.
 */
@Tags({"snowflake", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Snowflake Connection Pooling Service. Connections can be asked from pool and returned after usage.")
@DynamicProperties({
    @DynamicProperty(name = "JDBC property name",
        value = "Snowflake JDBC property value",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Snowflake JDBC driver property name and value applied to JDBC connections."),
    @DynamicProperty(name = "SENSITIVE.JDBC property name",
        value = "Snowflake JDBC property value",
        expressionLanguageScope = ExpressionLanguageScope.NONE,
        description = "Snowflake JDBC driver property name prefixed with 'SENSITIVE.' handled as a sensitive property.")
})
@RequiresInstanceClassLoading
public class SnowflakeComputingConnectionPool extends AbstractControllerService implements DBCPService {
    /**
     * Property Name Prefix for Sensitive Dynamic Properties
     */
    protected static final String SENSITIVE_PROPERTY_PREFIX = "SENSITIVE.";
    /*
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_IDLE} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MIN_IDLE = "0";
    /*
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MAX_IDLE} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MAX_IDLE = "8";
    /*
     * Copied from private variable {@link BasicDataSource.maxConnLifetimeMillis} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";
    /*
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);
    /*
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     * and converted from 1800000L to "1800000 millis" to "30 mins"
     */
    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";
    /*
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    public static final PropertyDescriptor SNOWFLAKE_URL = new PropertyDescriptor.Builder()
        .displayName("Snowflake URL")
        .name("snowflake-url")
        .description("E.g. 'cb56215.europe-west2.gcp.snowflakecomputing.com/?db=MY_DB'." +
            " The '/?db=MY_DB' part can can have other connection parameters as well." +
            " It can also be omitted but in that case tables need to be referenced with fully qualified names e.g. 'MY_DB.PUBLIC.MY_TABLe'.")
        .defaultValue(null)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor SNOWFLAKE_USER = new PropertyDescriptor.Builder()
        .displayName("Snowflake User Name")
        .name("snowflake-user")
        .defaultValue(null)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor SNOWFLAKE_PASSWORD = new PropertyDescriptor.Builder()
        .displayName("Snowflake Password")
        .name("snowflake-password")
        .defaultValue(null)
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
        .displayName("Validation query")
        .name("validation-query")
        .description("Validation query used to validate connections before returning them. "
            + "When connection is invalid, it gets dropped and new valid connection will be returned. "
            + "Note!! Using validation might have some performance penalty.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
        .displayName("Max Wait Time")
        .name("max-wait-time")
        .description("The maximum amount of time that the pool will wait (when there are no available connections) "
            + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
        .defaultValue("500 millis")
        .required(true)
        .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .sensitive(false)
        .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
        .displayName("Max Total Connections")
        .name("max-total-connections")
        .description("The maximum number of active connections that can be allocated from this pool at the same time, "
            + " or negative for no limit.")
        .defaultValue("8")
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .sensitive(false)
        .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
        .displayName("Minimum Idle Connections")
        .name("snowflake-min-idle-conns")
        .description("The minimum number of connections that can remain idle in the pool without extra ones being " +
            "created. Set to or zero to allow no idle connections.")
        .defaultValue(DEFAULT_MIN_IDLE)
        .required(false)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
        .displayName("Max Idle Connections")
        .name("snowflake-max-idle-conns")
        .description("The maximum number of connections that can remain idle in the pool without extra ones being " +
            "released. Set to any negative value to allow unlimited idle connections.")
        .defaultValue(DEFAULT_MAX_IDLE)
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
        .displayName("Max Connection Lifetime")
        .name("snowflake-max-conn-lifetime")
        .description("The maximum lifetime in milliseconds of a connection. After this time is exceeded the " +
            "connection will fail the next activation, passivation or validation test. A value of zero or less " +
            "means the connection has an infinite lifetime.")
        .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
        .required(false)
        .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
        .displayName("Time Between Eviction Runs")
        .name("snowflake-time-between-eviction-runs")
        .description("The number of milliseconds to sleep between runs of the idle connection evictor thread. When " +
            "non-positive, no idle connection evictor thread will be run.")
        .defaultValue(DEFAULT_EVICTION_RUN_PERIOD)
        .required(false)
        .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
        .displayName("Minimum Evictable Idle Time")
        .name("snowflake-min-evictable-idle-time")
        .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
        .defaultValue(DEFAULT_MIN_EVICTABLE_IDLE_TIME)
        .required(false)
        .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
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
        .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SNOWFLAKE_URL);
        props.add(SNOWFLAKE_USER);
        props.add(SNOWFLAKE_PASSWORD);
        props.add(VALIDATION_QUERY);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
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
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .dynamic(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR);

        if (propertyDescriptorName.startsWith(SENSITIVE_PROPERTY_PREFIX)) {
            builder.sensitive(true).expressionLanguageSupported(ExpressionLanguageScope.NONE);
        } else {
            builder.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY);
        }

        return builder.build();
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
        final String snowflakeUrl = context.getProperty(SNOWFLAKE_URL).evaluateAttributeExpressions().getValue();
        final String connectionString;
        if (snowflakeUrl.startsWith("jdbc:snowflake")) {
            connectionString = snowflakeUrl;
        } else {
            connectionString = "jdbc:snowflake://" + snowflakeUrl;
        }
        final String user = context.getProperty(SNOWFLAKE_USER).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(SNOWFLAKE_PASSWORD).evaluateAttributeExpressions().getValue();

        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());

        dataSource = new BasicDataSource();

        dataSource.setDriver(getDriver(connectionString));

        dataSource.setUrl(connectionString);
        dataSource.setUsername(user);
        dataSource.setPassword(password);

        dataSource.setMaxWaitMillis(maxWaitMillis);
        dataSource.setMaxTotal(maxTotal);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis);
        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);

        if (validationQuery != null && !validationQuery.isEmpty()) {
            dataSource.setValidationQuery(validationQuery);
            dataSource.setTestOnBorrow(true);
        }

        final List<PropertyDescriptor> dynamicProperties = context.getProperties()
            .keySet()
            .stream()
            .filter(PropertyDescriptor::isDynamic)
            .collect(Collectors.toList());

        dynamicProperties.forEach((descriptor) -> {
            final PropertyValue propertyValue = context.getProperty(descriptor);
            if (descriptor.isSensitive()) {
                final String propertyName = StringUtils.substringAfter(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX);
                dataSource.addConnectionProperty(propertyName, propertyValue.getValue());
            } else {
                dataSource.addConnectionProperty(descriptor.getName(), propertyValue.evaluateAttributeExpressions().getValue());
            }
        });
    }

    private Driver getDriver(final String url) {
        try {
            return DriverManager.getDriver(url);
        } catch (final SQLException e) {
            // In case the driver is not registered by the implementation, we explicitly try to register it.
            try {
                DriverManager.registerDriver(SnowflakeDriver.class.newInstance());
                return DriverManager.getDriver(url);
            } catch (final SQLException e2) {
                throw new ProcessException("No suitable driver for the given Database Connection URL", e2);
            } catch (final IllegalAccessException | InstantiationException e2) {
                throw new ProcessException("Creating driver instance is failed", e2);
            }
        }
    }

    private Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown pool, close all open connections.
     * *
     *
     * @throws SQLException if there is an error while closing open connections
     */
    @OnDisabled
    public void shutdown() throws SQLException {
        try {
            if (dataSource != null) {
                dataSource.close();
            }
        } finally {
            dataSource = null;
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
        return this.getClass().getSimpleName() + "[id=" + getIdentifier() + "]";
    }

    BasicDataSource getDataSource() {
        return dataSource;
    }
}
