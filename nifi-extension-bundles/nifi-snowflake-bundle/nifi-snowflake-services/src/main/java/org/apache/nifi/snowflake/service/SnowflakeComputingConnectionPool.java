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

import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.SnowflakeDriver;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.AbstractDBCPConnectionPool;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.snowflake.SnowflakeConnectionProviderService;
import org.apache.nifi.processors.snowflake.SnowflakeConnectionWrapper;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.snowflake.service.util.ConnectionUrlFormat;
import org.apache.nifi.snowflake.service.util.ConnectionUrlFormatParameters;

import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static net.snowflake.client.core.SFSessionProperty.AUTHENTICATOR;
import static net.snowflake.client.core.SFSessionProperty.PRIVATE_KEY_BASE64;
import static net.snowflake.client.core.SFSessionProperty.TOKEN;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_USER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_TOTAL_CONNECTIONS;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_WAIT_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.apache.nifi.dbcp.utils.DBCPProperties.extractMillisWithInfinite;

/**
 * Implementation of Database Connection Pooling Service for Snowflake. Apache DBCP is used for connection pooling
 * functionality.
 */
@Tags({"snowflake", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Snowflake Connection Pooling Service. Connections can be asked from pool and returned after usage.")
@SupportsSensitiveDynamicProperties
@DynamicProperties({
        @DynamicProperty(name = "JDBC property name",
                value = "Snowflake JDBC property value",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "Snowflake JDBC driver property name and value applied to JDBC connections.")
})
@RequiresInstanceClassLoading
public class SnowflakeComputingConnectionPool extends AbstractDBCPConnectionPool implements SnowflakeConnectionProviderService {

    public static final PropertyDescriptor CONNECTION_URL_FORMAT = new PropertyDescriptor.Builder()
            .name("Connection URL Format")
            .description("The format of the connection URL.")
            .allowableValues(ConnectionUrlFormat.class)
            .required(true)
            .defaultValue(ConnectionUrlFormat.FULL_URL)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_URL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DBCPProperties.DATABASE_URL)
            .displayName("Snowflake URL")
            .description("Example connection string: jdbc:snowflake://[account].[region]" + ConnectionUrlFormat.SNOWFLAKE_HOST_SUFFIX + "/?[connection_params]" +
                    " The connection parameters can include db=DATABASE_NAME to avoid using qualified table names such as DATABASE_NAME.PUBLIC.TABLE_NAME")
            .required(true)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.FULL_URL)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_ACCOUNT_LOCATOR = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_LOCATOR)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_CLOUD_REGION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_REGION)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_CLOUD_TYPE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_TYPE)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_ORGANIZATION_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ORGANIZATION_NAME)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_NAME)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_USER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DBCPProperties.DB_USER)
            .displayName("Username")
            .description("The Snowflake user name.")
            .build();

    public static final PropertyDescriptor SNOWFLAKE_PASSWORD = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DBCPProperties.DB_PASSWORD)
            .description("The password for the Snowflake user.")
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("Private Key Service")
            .description("Provides RSA Private Key for Key Pair Authentication")
            .identifiesControllerService(PrivateKeyService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("OAuth2 Access Token Provider")
            .description("Service providing OAuth2 Access Tokens for authenticating using the HTTP Authorization Header")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .build();

    public static final PropertyDescriptor SNOWFLAKE_WAREHOUSE = new PropertyDescriptor.Builder()
            .name("Warehouse")
            .description("The warehouse to use by default. The same as passing 'warehouse=WAREHOUSE' to the connection string.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final String AUTHENTICATOR_SNOWFLAKE_JWT = "SNOWFLAKE_JWT";

    private static final String PEM_CONTENT_FORMAT = "-----BEGIN PRIVATE KEY-----%n%s%n-----END PRIVATE KEY-----%n";

    private volatile OAuth2AccessTokenProvider accessTokenProvider;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_URL_FORMAT,
            SNOWFLAKE_URL,
            SNOWFLAKE_ACCOUNT_LOCATOR,
            SNOWFLAKE_CLOUD_REGION,
            SNOWFLAKE_CLOUD_TYPE,
            SNOWFLAKE_ORGANIZATION_NAME,
            SNOWFLAKE_ACCOUNT_NAME,
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SnowflakeProperties.DATABASE,
            SnowflakeProperties.SCHEMA,
            SNOWFLAKE_WAREHOUSE,
            PRIVATE_KEY_SERVICE,
            ACCESS_TOKEN_PROVIDER,
            ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE,
            VALIDATION_QUERY,
            MAX_WAIT_TIME,
            MAX_TOTAL_CONNECTIONS,
            MIN_IDLE,
            MAX_IDLE,
            MAX_CONN_LIFETIME,
            EVICTION_RUN_PERIOD,
            MIN_EVICTABLE_IDLE_TIME,
            SOFT_MIN_EVICTABLE_IDLE_TIME
    );

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("connection-url-format", CONNECTION_URL_FORMAT.getName());
        config.renameProperty("warehouse", SNOWFLAKE_WAREHOUSE.getName());
        config.renameProperty(SnowflakeProperties.OLD_ACCOUNT_LOCATOR_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_LOCATOR.getName());
        config.renameProperty(SnowflakeProperties.OLD_CLOUD_REGION_PROPERTY_NAME, SnowflakeProperties.CLOUD_REGION.getName());
        config.renameProperty(SnowflakeProperties.OLD_CLOUD_TYPE_PROPERTY_NAME, SnowflakeProperties.CLOUD_TYPE.getName());
        config.renameProperty(SnowflakeProperties.OLD_ORGANIZATION_NAME_PROPERTY_NAME, SnowflakeProperties.ORGANIZATION_NAME.getName());
        config.renameProperty(SnowflakeProperties.OLD_ACCOUNT_NAME_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_NAME.getName());
        config.renameProperty(SnowflakeProperties.OLD_DATABASE_PROPERTY_NAME, SnowflakeProperties.DATABASE.getName());
        config.renameProperty(SnowflakeProperties.OLD_SCHEMA_PROPERTY_NAME, SnowflakeProperties.SCHEMA.getName());
        config.renameProperty(DBCPProperties.OLD_VALIDATION_QUERY_PROPERTY_NAME, VALIDATION_QUERY.getName());
        config.renameProperty(DBCPProperties.OLD_MIN_IDLE_PROPERTY_NAME, MIN_IDLE.getName());
        config.renameProperty(DBCPProperties.OLD_MAX_IDLE_PROPERTY_NAME, MAX_IDLE.getName());
        config.renameProperty(DBCPProperties.OLD_MAX_CONN_LIFETIME_PROPERTY_NAME, MAX_CONN_LIFETIME.getName());
        config.renameProperty(DBCPProperties.OLD_EVICTION_RUN_PERIOD_PROPERTY_NAME, EVICTION_RUN_PERIOD.getName());
        config.renameProperty(DBCPProperties.OLD_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, MIN_EVICTABLE_IDLE_TIME.getName());
        config.renameProperty(DBCPProperties.OLD_SOFT_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, SOFT_MIN_EVICTABLE_IDLE_TIME.getName());
        config.renameProperty(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR);

        return builder.build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        return Collections.emptyList();
    }

    @OnEnabled
    @Override
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        super.onConfigured(context);
        accessTokenProvider = context.getProperty(ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
    }

    @OnDisabled
    public void onDisabled() {
        accessTokenProvider = null;
    }

    private void refreshAccessToken() {
        if (accessTokenProvider != null) {
            dataSource.addConnectionProperty(TOKEN.getPropertyKey(), accessTokenProvider.getAccessDetails().getAccessToken());
        }
    }

    @Override
    protected DataSourceConfiguration getDataSourceConfiguration(final ConfigurationContext context) {
        final String url = getUrl(context);
        final String driverName = SnowflakeDriver.class.getName();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());

        return new DataSourceConfiguration.Builder(url, driverName, user, password)
                .maxTotal(maxTotal)
                .validationQuery(validationQuery)
                .maxWaitMillis(maxWaitMillis)
                .minIdle(minIdle)
                .maxIdle(maxIdle)
                .maxConnLifetimeMillis(maxConnLifetimeMillis)
                .timeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis)
                .minEvictableIdleTimeMillis(minEvictableIdleTimeMillis)
                .softMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis)
                .build();
    }

    protected String getUrl(final ConfigurationContext context) {
        final ConnectionUrlFormat connectionUrlFormat = context.getProperty(CONNECTION_URL_FORMAT).asAllowableValue(ConnectionUrlFormat.class);
        final ConnectionUrlFormatParameters parameters = getConnectionUrlFormatParameters(context);

        return connectionUrlFormat.buildConnectionUrl(parameters);
    }

    @Override
    protected Driver getDriver(final String driverName, final String url) {
        try {
            Class.forName(driverName);
            return DriverManager.getDriver(url);
        } catch (Exception e) {
            throw new ProcessException("Snowflake driver unavailable or incompatible connection URL", e);
        }
    }

    @Override
    protected Map<String, String> getConnectionProperties(final ConfigurationContext context) {
        final String database = context.getProperty(SnowflakeProperties.DATABASE).evaluateAttributeExpressions().getValue();
        final String schema = context.getProperty(SnowflakeProperties.SCHEMA).evaluateAttributeExpressions().getValue();
        final String warehouse = context.getProperty(SNOWFLAKE_WAREHOUSE).evaluateAttributeExpressions().getValue();
        final OAuth2AccessTokenProvider tokenProvider = context.getProperty(ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

        final Map<String, String> connectionProperties = super.getConnectionProperties(context);
        if (database != null) {
            connectionProperties.put("db", database);
        }
        if (schema != null) {
            connectionProperties.put("schema", schema);
        }
        if (warehouse != null) {
            connectionProperties.put("warehouse", warehouse);
        }
        if (tokenProvider != null) {
            connectionProperties.put(AUTHENTICATOR.getPropertyKey(), "oauth");
            connectionProperties.put(TOKEN.getPropertyKey(), tokenProvider.getAccessDetails().getAccessToken());
        }

        final PropertyValue privateKeyServiceProperty = context.getProperty(PRIVATE_KEY_SERVICE);
        if (privateKeyServiceProperty.isSet()) {
            final PrivateKeyService privateKeyService = privateKeyServiceProperty.asControllerService(PrivateKeyService.class);
            final PrivateKey privateKey = privateKeyService.getPrivateKey();
            final String privateKeyBase64 = getPrivateKeyBase64(privateKey);
            connectionProperties.put(PRIVATE_KEY_BASE64.getPropertyKey(), privateKeyBase64);
            connectionProperties.put(AUTHENTICATOR.getPropertyKey(), AUTHENTICATOR_SNOWFLAKE_JWT);
        }

        final ProxyConfigurationService proxyConfigurationService = context
                .getProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE)
                .asControllerService(ProxyConfigurationService.class);
        if (proxyConfigurationService != null) {
            final ProxyConfiguration proxyConfiguration = proxyConfigurationService.getConfiguration();
            connectionProperties.put(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
            if (proxyConfiguration.getProxyServerHost() != null) {
                connectionProperties.put(SFSessionProperty.PROXY_HOST.getPropertyKey(), proxyConfiguration.getProxyServerHost());
            }
            if (proxyConfiguration.getProxyServerPort() != null) {
                connectionProperties.put(SFSessionProperty.PROXY_PORT.getPropertyKey(), proxyConfiguration.getProxyServerPort().toString());
            }
            if (proxyConfiguration.getProxyUserName() != null) {
                connectionProperties.put(SFSessionProperty.PROXY_USER.getPropertyKey(), proxyConfiguration.getProxyUserName());
            }
            if (proxyConfiguration.getProxyUserPassword() != null) {
                connectionProperties.put(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), proxyConfiguration.getProxyUserPassword());
            }
            if (proxyConfiguration.getProxyType() != null) {
                connectionProperties.put(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey(), proxyConfiguration.getProxyType().name().toLowerCase());
            }
        }
        return connectionProperties;
    }

    @Override
    public Connection getConnection() throws ProcessException {
        refreshAccessToken();
        return super.getConnection();
    }

    @Override
    public SnowflakeConnectionWrapper getSnowflakeConnection() {
        return new SnowflakeConnectionWrapper(getConnection());
    }

    private ConnectionUrlFormatParameters getConnectionUrlFormatParameters(ConfigurationContext context) {
        return new ConnectionUrlFormatParameters(
                context.getProperty(SNOWFLAKE_URL).evaluateAttributeExpressions().getValue(),
                context.getProperty(SNOWFLAKE_ORGANIZATION_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(SNOWFLAKE_ACCOUNT_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(SNOWFLAKE_ACCOUNT_LOCATOR).evaluateAttributeExpressions().getValue(),
                context.getProperty(SNOWFLAKE_CLOUD_REGION).evaluateAttributeExpressions().getValue(),
                context.getProperty(SNOWFLAKE_CLOUD_TYPE).evaluateAttributeExpressions().getValue()
        );
    }

    private String getPrivateKeyBase64(final PrivateKey privateKey) {
        final byte[] privateKeyEncoded = privateKey.getEncoded();
        final Base64.Encoder encoder = Base64.getEncoder();
        final String privateKeyEncodedBase64 = encoder.encodeToString(privateKeyEncoded);
        final String pemPrivateKey = PEM_CONTENT_FORMAT.formatted(privateKeyEncodedBase64);
        final byte[] pemPrivateKeyBinary = pemPrivateKey.getBytes(StandardCharsets.UTF_8);
        return encoder.encodeToString(pemPrivateKeyBinary);
    }
}
