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

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.SnowflakeDriver;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.AbstractDBCPConnectionPool;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.snowflake.SnowflakeConnectionProviderService;
import org.apache.nifi.processors.snowflake.SnowflakeConnectionWrapper;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.snowflake.service.util.CommonProperties;
import org.apache.nifi.snowflake.service.util.ConnectionUrlFormat;

/**
 * Implementation of Database Connection Pooling Service for Snowflake. Apache DBCP is used for connection pooling
 * functionality.
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
public class SnowflakeComputingConnectionPool extends AbstractDBCPConnectionPool implements SnowflakeConnectionProviderService {

    public static final PropertyDescriptor CONNECTION_URL_FORMAT = new PropertyDescriptor.Builder()
            .name("connection-url-format")
            .displayName("Connection URL Format")
            .description("The format of the connection URL.")
            .allowableValues(ConnectionUrlFormat.class)
            .required(true)
            .defaultValue(ConnectionUrlFormat.FULL_URL.getValue())
            .build();

    public static final PropertyDescriptor SNOWFLAKE_URL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractDBCPConnectionPool.DATABASE_URL)
            .displayName("Snowflake URL")
            .description("Example connection string: jdbc:snowflake://[account].[region].snowflakecomputing.com/?[connection_params]" +
                    " The connection parameters can include db=DATABASE_NAME to avoid using qualified table names such as DATABASE_NAME.PUBLIC.TABLE_NAME")
            .required(true)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.FULL_URL)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_ACCOUNT_LOCATOR = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CommonProperties.ACCOUNT_LOCATOR)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_CLOUD_REGION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CommonProperties.CLOUD_REGION)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_CLOUD_TYPE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CommonProperties.CLOUD_TYPE)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_ORGANIZATION_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CommonProperties.ORGANIZATION_NAME)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CommonProperties.ACCOUNT_NAME)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_USER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractDBCPConnectionPool.DB_USER)
            .displayName("Snowflake User")
            .description("The Snowflake user name.")
            .required(true)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_PASSWORD = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractDBCPConnectionPool.DB_PASSWORD)
            .displayName("Snowflake Password")
            .description("The password for the Snowflake user.")
            .required(true)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_DATABASE = new PropertyDescriptor.Builder()
            .name("database")
            .displayName("Database")
            .description("The database to use by default. The same as passing 'db=DATABASE_NAME' to the connection string.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_SCHEMA = new PropertyDescriptor.Builder()
            .name("schema")
            .displayName("Schema")
            .description("The schema to use by default. The same as passing 'schema=SCHEMA' to the connection string.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("PUBLIC")
            .build();

    public static final PropertyDescriptor SNOWFLAKE_WAREHOUSE = new PropertyDescriptor.Builder()
            .name("warehouse")
            .displayName("Warehouse")
            .description("The warehouse to use by default. The same as passing 'warehouse=WAREHOUSE' to the connection string.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONNECTION_URL_FORMAT);
        props.add(SNOWFLAKE_URL);
        props.add(SNOWFLAKE_ACCOUNT_LOCATOR);
        props.add(SNOWFLAKE_CLOUD_REGION);
        props.add(SNOWFLAKE_CLOUD_TYPE);
        props.add(SNOWFLAKE_ORGANIZATION_NAME);
        props.add(SNOWFLAKE_ACCOUNT_NAME);
        props.add(SNOWFLAKE_USER);
        props.add(SNOWFLAKE_PASSWORD);
        props.add(SNOWFLAKE_DATABASE);
        props.add(SNOWFLAKE_SCHEMA);
        props.add(SNOWFLAKE_WAREHOUSE);
        props.add(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE);
        props.add(VALIDATION_QUERY);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);

        PROPERTIES = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        return Collections.emptyList();
    }

    @Override
    protected String getUrl(final ConfigurationContext context) {
        final ConnectionUrlFormat connectionUrlFormat = ConnectionUrlFormat.forName(context.getProperty(CONNECTION_URL_FORMAT)
                .getValue());
        return connectionUrlFormat.buildConnectionUrl(context);
    }

    @Override
    protected Driver getDriver(final String driverName, final String url) {
        try {
            Class.forName(SnowflakeDriver.class.getName());
            return DriverManager.getDriver(url);
        } catch (Exception e) {
            throw new ProcessException("Snowflake driver unavailable or incompatible connection URL", e);
        }
    }

    @Override
    protected Map<String, String> getConnectionProperties(final ConfigurationContext context) {
        final String database = context.getProperty(SNOWFLAKE_DATABASE).evaluateAttributeExpressions().getValue();
        final String schema = context.getProperty(SNOWFLAKE_SCHEMA).evaluateAttributeExpressions().getValue();
        final String warehouse = context.getProperty(SNOWFLAKE_WAREHOUSE).evaluateAttributeExpressions().getValue();

        final Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put("db", database);
        connectionProperties.put("schema", schema);
        if (warehouse != null) {
            connectionProperties.put("warehouse", warehouse);
        }

        final ProxyConfigurationService proxyConfigurationService = context
                .getProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE)
                .asControllerService(ProxyConfigurationService.class);
        if (proxyConfigurationService != null) {
            final ProxyConfiguration proxyConfiguration = proxyConfigurationService.getConfiguration();
            connectionProperties.put(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
            connectionProperties.put(SFSessionProperty.PROXY_HOST.getPropertyKey(), proxyConfiguration.getProxyServerHost());
            connectionProperties.put(SFSessionProperty.PROXY_PORT.getPropertyKey(), proxyConfiguration.getProxyServerPort().toString());
            connectionProperties.put(SFSessionProperty.PROXY_USER.getPropertyKey(), proxyConfiguration.getProxyUserName());
            connectionProperties.put(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), proxyConfiguration.getProxyUserPassword());
            connectionProperties.put(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey(), proxyConfiguration.getProxyType().name().toLowerCase());
        }
        return connectionProperties;
    }

    @Override
    public SnowflakeConnectionWrapper getSnowflakeConnection() {
        return new SnowflakeConnectionWrapper(getConnection());
    }
}
