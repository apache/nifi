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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.dbcp.utils.DriverUtils;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.nifi.dbcp.utils.DBCPProperties.DATABASE_URL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVERNAME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVER_LOCATION;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD_PROVIDER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD_WITH_PASSWORD_SOURCE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_USER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_TOTAL_CONNECTIONS;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_WAIT_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.PASSWORD_SOURCE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.PasswordSource.PASSWORD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.PasswordSource.PASSWORD_PROVIDER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.apache.nifi.dbcp.utils.DBCPProperties.extractMillisWithInfinite;

/**
 * Implementation of for Database Connection Pooling Service. Apache DBCP is used for connection pooling functionality.
 */
@SupportsSensitiveDynamicProperties
@Tags({"dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage.")
@DynamicProperties({
        @DynamicProperty(name = "JDBC property name",
                value = "JDBC property value",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "JDBC driver property name and value applied to JDBC connections."),
        @DynamicProperty(name = "SENSITIVE.JDBC property name",
                value = "JDBC property value",
                expressionLanguageScope = ExpressionLanguageScope.NONE,
                description = "JDBC driver property name prefixed with 'SENSITIVE.' handled as a sensitive property.")
})
@RequiresInstanceClassLoading
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Database Driver Location can reference resources over HTTP"
                )
        }
)
public class DBCPConnectionPool extends AbstractDBCPConnectionPool implements DBCPService, VerifiableControllerService {
    /**
     * Property Name Prefix for Sensitive Dynamic Properties
     */
    protected static final String SENSITIVE_PROPERTY_PREFIX = "SENSITIVE.";

    // Hold an instance of the registered driver so we can properly de-register it.
    private volatile Driver registeredDriver;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        DATABASE_URL,
        DB_DRIVERNAME,
        DB_DRIVER_LOCATION,
        KERBEROS_USER_SERVICE,
        DB_USER,
        PASSWORD_SOURCE,
        DB_PASSWORD_WITH_PASSWORD_SOURCE,
        DB_PASSWORD_PROVIDER,
        MAX_WAIT_TIME,
        MAX_TOTAL_CONNECTIONS,
        VALIDATION_QUERY,
        MIN_IDLE,
        MAX_IDLE,
        MAX_CONN_LIFETIME,
        EVICTION_RUN_PERIOD,
        MIN_EVICTABLE_IDLE_TIME,
        SOFT_MIN_EVICTABLE_IDLE_TIME
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.removeProperty("kerberos-principal");
        config.removeProperty("kerberos-password");
        config.removeProperty("kerberos-credentials-service");
        DBCPProperties.OLD_DB_DRIVER_LOCATION_PROPERTY_NAMES.forEach(oldPropertyName ->
                config.renameProperty(oldPropertyName, DB_DRIVER_LOCATION.getName())
        );
        config.renameProperty(DBCPProperties.OLD_VALIDATION_QUERY_PROPERTY_NAME, VALIDATION_QUERY.getName());
        config.renameProperty(DBCPProperties.OLD_MIN_IDLE_PROPERTY_NAME, MIN_IDLE.getName());
        config.renameProperty(DBCPProperties.OLD_MAX_IDLE_PROPERTY_NAME, MAX_IDLE.getName());
        config.renameProperty(DBCPProperties.OLD_MAX_CONN_LIFETIME_PROPERTY_NAME, MAX_CONN_LIFETIME.getName());
        config.renameProperty(DBCPProperties.OLD_EVICTION_RUN_PERIOD_PROPERTY_NAME, EVICTION_RUN_PERIOD.getName());
        config.renameProperty(DBCPProperties.OLD_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, MIN_EVICTABLE_IDLE_TIME.getName());
        config.renameProperty(DBCPProperties.OLD_SOFT_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, SOFT_MIN_EVICTABLE_IDLE_TIME.getName());
        config.renameProperty(DBCPProperties.OLD_KERBEROS_USER_SERVICE_PROPERTY_NAME, KERBEROS_USER_SERVICE.getName());
    }

    BasicDataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected DataSourceConfiguration getDataSourceConfiguration(ConfigurationContext context) {
        final String url = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final PropertyValue passwordSourceProperty = context.getProperty(PASSWORD_SOURCE);
        final String passwordSource = passwordSourceProperty == null ? PASSWORD.getValue() : passwordSourceProperty.getValue();
        final boolean passwordProviderSelected = PASSWORD_PROVIDER.getValue().equals(passwordSource);
        final String password = passwordProviderSelected ? null : context.getProperty(DB_PASSWORD_WITH_PASSWORD_SOURCE).evaluateAttributeExpressions().getValue();
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
            builder.expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT);
        }

        return builder.build();
    }

    @Override
    protected Map<String, String> getConnectionProperties(ConfigurationContext context) {
        return getDynamicProperties(context)
                .stream()
                .map(descriptor -> {
                    final PropertyValue propertyValue = context.getProperty(descriptor);
                    if (descriptor.isSensitive()) {
                        final String propertyName;
                        if (Strings.CS.startsWith(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX)) {
                            propertyName = StringUtils.substringAfter(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX);
                        } else {
                            propertyName = descriptor.getName();
                        }
                        return new AbstractMap.SimpleEntry<>(propertyName, propertyValue.getValue());
                    } else {
                        return new AbstractMap.SimpleEntry<>(descriptor.getName(), propertyValue.evaluateAttributeExpressions().getValue());
                    }
                })
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    @Override
    protected Driver getDriver(final String driverName, final String url) {
        final Class<?> clazz;

        try {
            clazz = Class.forName(driverName);
        } catch (final ClassNotFoundException e) {
            // Enhanced error message with discovery
            ResourceReferences driverResources = null;
            try {
                // Try to get driver resources from current context if available
                if (getConfigurationContext() != null) {
                    driverResources = getConfigurationContext().getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions().asResources();
                }
            } catch (Exception ignored) {
                // Context might not be available, continue without it
            }

            final List<String> availableDrivers = (driverResources != null && driverResources.getCount() != 0) ? DriverUtils.discoverDriverClasses(driverResources) : List.of();

            StringBuilder errorMessage = new StringBuilder("JDBC driver class '%s' not found.".formatted(driverName));

            if (!availableDrivers.isEmpty()) {
                errorMessage.append(" Available driver classes found in resources: %s.".formatted(String.join(", ", availableDrivers)));
            } else if (driverResources != null && driverResources.getCount() != 0) {
                final List<ResourceReference> resourcesList = driverResources.asList();
                if (resourcesList.stream().filter(r -> r.getResourceType() != ResourceType.URL).count() != 0) {
                    errorMessage.append(" No JDBC driver classes found in the provided resources.");
                }
            } else if (driverResources == null) {
                errorMessage.append(" The property 'Database Driver Location(s)' should be set.");
            }

            throw new ProcessException(errorMessage.toString(), e);
        }

        try {
            final Driver driver = DriverManager.getDriver(url);
            // Ensure drivers that register themselves during class loading can be set as the registeredDriver.
            // This ensures drivers that register themselves can be deregistered when the component is removed.
            if (driver != registeredDriver) {
                DriverManager.deregisterDriver(registeredDriver);

                registeredDriver = driver;
                if (!registeredDriver.getClass().getClassLoader().equals(getClass().getClassLoader())) {
                    getLogger().warn("Registered Driver [{}] created in different ClassLoader: Driver will become unavailable when deregistered", registeredDriver);
                }
            }
            return driver;
        } catch (final SQLException e) {
            // In case the driver is not registered by the implementation, we explicitly try to register it.
            // deregister existing driver
            try {
                DriverManager.deregisterDriver(registeredDriver);
                registeredDriver = (Driver) clazz.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(registeredDriver);
                return DriverManager.getDriver(url);
            } catch (final SQLException e2) {
                throw new ProcessException("No suitable driver for the given Database Connection URL", e2);
            } catch (final Exception e2) {
                throw new ProcessException("Creating driver instance is failed", e2);
            }
        }
    }

    @OnRemoved
    public void onRemove() {
        try {
            // We need to deregister the driver to allow the InstanceClassLoader to be garbage collected.
            DriverManager.deregisterDriver(registeredDriver);
        } catch (SQLException e) {
            getLogger().warn("Potential memory leak: Driver could not be deregistered [{}]", registeredDriver, e);
        }
    }
}
