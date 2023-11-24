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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;

import static org.apache.nifi.dbcp.utils.DBCPProperties.DATABASE_URL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVERNAME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVER_LOCATION;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_USER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;
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

    private static final List<PropertyDescriptor> PROPERTIES;

    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("kerberos-principal")
            .displayName("Kerberos Principal")
            .description("The principal to use when specifying the principal and password directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor KERBEROS_PASSWORD = new PropertyDescriptor.Builder()
            .name("kerberos-password")
            .displayName("Kerberos Password")
            .description("The password to use when specifying the principal and password directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(DB_DRIVERNAME);
        props.add(DB_DRIVER_LOCATION);
        props.add(KERBEROS_USER_SERVICE);
        props.add(KERBEROS_CREDENTIALS_SERVICE);
        props.add(KERBEROS_PRINCIPAL);
        props.add(KERBEROS_PASSWORD);
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

        PROPERTIES = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final boolean kerberosPrincipalProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue());
        final boolean kerberosPasswordProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PASSWORD).getValue());

        if (kerberosPrincipalProvided && !kerberosPasswordProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PASSWORD.getDisplayName())
                    .valid(false)
                    .explanation("a password must be provided for the given principal")
                    .build());
        }

        if (kerberosPasswordProvided && !kerberosPrincipalProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PRINCIPAL.getDisplayName())
                    .valid(false)
                    .explanation("a principal must be provided for the given password")
                    .build());
        }

        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosCredentialsService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_CREDENTIALS_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos principal/password and kerberos credential service cannot be configured at the same time")
                    .build());
        }

        if (kerberosUserService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_USER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos principal/password and kerberos user service cannot be configured at the same time")
                    .build());
        }

        if (kerberosUserService != null && kerberosCredentialsService != null) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_USER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos user service and kerberos credential service cannot be configured at the same time")
                    .build());
        }

        return results;
    }

    BasicDataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected DataSourceConfiguration getDataSourceConfiguration(ConfigurationContext context) {
        final String url = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
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
                        if (StringUtils.startsWith(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX)) {
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
            throw new ProcessException("Driver class " + driverName + " is not found", e);
        }

        try {
            return DriverManager.getDriver(url);
        } catch (final SQLException e) {
            // In case the driver is not registered by the implementation, we explicitly try to register it.
            try {
                final Driver driver = (Driver) clazz.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(driver);
                return DriverManager.getDriver(url);
            } catch (final SQLException e2) {
                throw new ProcessException("No suitable driver for the given Database Connection URL", e2);
            } catch (final Exception e2) {
                throw new ProcessException("Creating driver instance is failed", e2);
            }
        }
    }

    @Override
    protected KerberosUser getKerberosUserByCredentials(ConfigurationContext context) {
        KerberosUser kerberosUser = super.getKerberosUserByCredentials(context);
        if (kerberosUser == null) {
            final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
            final String kerberosPrincipal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
            final String kerberosPassword = context.getProperty(KERBEROS_PASSWORD).getValue();
            if (kerberosCredentialsService != null) {
                kerberosUser = new KerberosKeytabUser(kerberosCredentialsService.getPrincipal(), kerberosCredentialsService.getKeytab());
            } else if (!StringUtils.isBlank(kerberosPrincipal) && !StringUtils.isBlank(kerberosPassword)) {
                kerberosUser = new KerberosPasswordUser(kerberosPrincipal, kerberosPassword);
            }
        }
        return kerberosUser;
    }
}
