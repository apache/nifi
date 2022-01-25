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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPConnectionPool;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
public class SnowflakeComputingConnectionPool extends DBCPConnectionPool {

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
        .fromPropertyDescriptor(DBCPConnectionPool.VALIDATION_QUERY)
        .displayName("Validation query")
        .name("validation-query")
        .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.MAX_WAIT_TIME)
        .displayName("Max Wait Time")
        .name("max-wait-time")
        .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.MAX_TOTAL_CONNECTIONS)
        .displayName("Max Total Connections")
        .name("max-total-connections")
        .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.MIN_IDLE)
        .displayName("Minimum Idle Connections")
        .name("snowflake-min-idle-conns")
        .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.MAX_IDLE)
        .displayName("Max Idle Connections")
        .name("snowflake-max-idle-conns")
        .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.MAX_CONN_LIFETIME)
        .displayName("Max Connection Lifetime")
        .name("snowflake-max-conn-lifetime")
        .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.EVICTION_RUN_PERIOD)
        .displayName("Time Between Eviction Runs")
        .name("snowflake-time-between-eviction-runs")
        .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.MIN_EVICTABLE_IDLE_TIME)
        .displayName("Minimum Evictable Idle Time")
        .name("snowflake-min-evictable-idle-time")
        .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.SOFT_MIN_EVICTABLE_IDLE_TIME)
        .displayName("Soft Minimum Evictable Idle Time")
        .name("snowflake-soft-min-evictable-idle-time")
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

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        return Collections.emptyList();
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

        dataSource.setDriver(getDriver(SnowflakeDriver.class.getName(), connectionString));

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

    protected Driver getDriver(final String driverName, final String url) {
        final Class<?> clazz;

        try {
            clazz = Class.forName(driverName);
        } catch (final ClassNotFoundException e) {
            throw new ProcessException("Driver class " + driverName +  " is not found", e);
        }

        try {
            return DriverManager.getDriver(url);
        } catch (final SQLException e) {
            // In case the driver is not registered by the implementation, we explicitly try to register it.
            try {
                final Driver driver = (Driver) clazz.newInstance();
                DriverManager.registerDriver(driver);
                return DriverManager.getDriver(url);
            } catch (final SQLException e2) {
                throw new ProcessException("No suitable driver for the given Database Connection URL", e2);
            } catch (final IllegalAccessException | InstantiationException e2) {
                throw new ProcessException("Creating driver instance is failed", e2);
            }
        }
    }

    /**
     * Shutdown pool, close all open connections.
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
            final Connection connection = dataSource.getConnection();
            return connection;
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[id=" + getIdentifier() + "]";
    }
}
