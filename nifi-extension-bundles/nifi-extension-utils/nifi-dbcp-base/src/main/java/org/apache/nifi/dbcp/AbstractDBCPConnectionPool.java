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
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.dbcp.utils.DriverUtils;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVERNAME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVER_LOCATION;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD_PROVIDER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.PASSWORD_SOURCE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.PasswordSource.PASSWORD_PROVIDER;

public abstract class AbstractDBCPConnectionPool extends AbstractControllerService implements DBCPService, VerifiableControllerService {

    protected volatile ProviderAwareBasicDataSource dataSource;
    protected volatile KerberosUser kerberosUser;

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        List<ConfigVerificationResult> results = new ArrayList<>();

        KerberosUser kerberosUser = null;

        try {
            kerberosUser = getKerberosUser(context);
            if (kerberosUser != null) {
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Configure Kerberos User")
                        .outcome(SUCCESSFUL)
                        .explanation("Successfully configured Kerberos user")
                        .build());
            }
        } catch (final Exception e) {
            verificationLogger.error("Failed to configure Kerberos user", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Kerberos User")
                    .outcome(FAILED)
                    .explanation("Failed to configure Kerberos user: " + e.getMessage())
                    .build());
        }

        final ProviderAwareBasicDataSource basicDataSource = new ProviderAwareBasicDataSource();
        try {
            final DataSourceConfiguration configuration = getDataSourceConfiguration(context);
            final Map<String, String> connectionProperties = getConnectionProperties(context);
            configureDataSource(context, basicDataSource, configuration, connectionProperties);

            final DatabasePasswordProvider passwordProvider = getDatabasePasswordProvider(context);
            final DatabasePasswordRequestContext passwordRequestContext = passwordProvider == null ? null :
                    buildDatabasePasswordRequestContext(configuration, connectionProperties);
            basicDataSource.setDatabasePasswordProvider(passwordProvider, passwordRequestContext);

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Data Source")
                    .outcome(SUCCESSFUL)
                    .explanation("Successfully configured data source")
                    .build());

            try (final Connection ignored = getConnection(basicDataSource, kerberosUser)) {
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Establish Connection")
                        .outcome(SUCCESSFUL)
                        .explanation("Successfully established Database Connection")
                        .build());
            } catch (final Exception e) {
                verificationLogger.error("Failed to establish Database Connection", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Establish Connection")
                        .outcome(FAILED)
                        .explanation("Failed to establish Database Connection: " + e.getMessage())
                        .build());
            }
        } catch (final Exception e) {
            StringBuilder messageBuilder = new StringBuilder("Failed to configure Data Source.");
            verificationLogger.error(messageBuilder.toString(), e);

            final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
            final ResourceReferences driverResources = context.getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions().asResources();

            if (StringUtils.isNotBlank(driverName) && driverResources.getCount() != 0) {
                List<String> availableDrivers = DriverUtils.findDriverClassNames(driverResources);
                if (!availableDrivers.isEmpty() && !availableDrivers.contains(driverName)) {
                    messageBuilder.append(String.format(" Driver class [%s] not found in provided resources. Available driver classes found: %s",
                            driverName, String.join(", ", availableDrivers)));
                } else if (e.getCause() instanceof ClassNotFoundException && availableDrivers.contains(driverName)) {
                    messageBuilder.append(" Driver Class found but not loaded: Apply configuration before verifying.");
                } else {
                    messageBuilder.append(String.format(" Exception: %s", e.getMessage()));
                }
            } else {
                messageBuilder.append(String.format(" No driver name specified or no driver resources provided. Exception: %s", e.getMessage()));
            }

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Data Source")
                    .outcome(FAILED)
                    .explanation(messageBuilder.toString())
                    .build());
        } finally {
            try {
                shutdown(basicDataSource, kerberosUser);
            } catch (final SQLException e) {
                verificationLogger.error("Failed to shut down data source", e);
            }
        }
        return results;
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
        dataSource = new ProviderAwareBasicDataSource();
        kerberosUser = getKerberosUser(context);
        loginKerberos(kerberosUser);
        final DataSourceConfiguration configuration = getDataSourceConfiguration(context);
        final Map<String, String> connectionProperties = getConnectionProperties(context);
        configureDataSource(context, dataSource, configuration, connectionProperties);

        final DatabasePasswordProvider passwordProvider = getDatabasePasswordProvider(context);
        final DatabasePasswordRequestContext passwordRequestContext = passwordProvider == null ? null :
                buildDatabasePasswordRequestContext(configuration, connectionProperties);
        dataSource.setDatabasePasswordProvider(passwordProvider, passwordRequestContext);
    }

    private void loginKerberos(KerberosUser kerberosUser) throws InitializationException {
        if (kerberosUser != null) {
            try {
                kerberosUser.login();
            } catch (KerberosLoginException e) {
                throw new InitializationException("Unable to authenticate Kerberos principal", e);
            }
        }
    }

    protected abstract Driver getDriver(final String driverName, final String url);

    protected abstract DataSourceConfiguration getDataSourceConfiguration(final ConfigurationContext context);

    protected void configureDataSource(final ConfigurationContext context, final BasicDataSource basicDataSource,
                                       final DataSourceConfiguration configuration, final Map<String, String> connectionProperties) {
        final Driver driver = getDriver(configuration.getDriverName(), configuration.getUrl());

        basicDataSource.setDriver(driver);
        basicDataSource.setMaxWait(Duration.ofMillis(configuration.getMaxWaitMillis()));
        basicDataSource.setMaxTotal(configuration.getMaxTotal());
        basicDataSource.setMinIdle(configuration.getMinIdle());
        basicDataSource.setMaxIdle(configuration.getMaxIdle());
        basicDataSource.setMaxConn(Duration.ofMillis(configuration.getMaxConnLifetimeMillis()));
        basicDataSource.setDurationBetweenEvictionRuns(Duration.ofMillis(configuration.getTimeBetweenEvictionRunsMillis()));
        basicDataSource.setMinEvictableIdle(Duration.ofMillis(configuration.getMinEvictableIdleTimeMillis()));
        basicDataSource.setSoftMinEvictableIdle(Duration.ofMillis(configuration.getSoftMinEvictableIdleTimeMillis()));

        final String validationQuery = configuration.getValidationQuery();
        if (StringUtils.isNotBlank(validationQuery)) {
            basicDataSource.setValidationQuery(validationQuery);
            basicDataSource.setTestOnBorrow(true);
        }

        basicDataSource.setUrl(configuration.getUrl());
        basicDataSource.setUsername(configuration.getUserName());
        basicDataSource.setPassword(configuration.getPassword());

        connectionProperties.forEach(basicDataSource::addConnectionProperty);
    }

    protected Map<String, String> getConnectionProperties(final ConfigurationContext context) {
        return getDynamicProperties(context)
                .stream()
                .collect(Collectors.toMap(PropertyDescriptor::getName, s -> {
                    final PropertyValue propertyValue = context.getProperty(s);
                    return propertyValue.evaluateAttributeExpressions().getValue();
                }));
    }

    protected List<PropertyDescriptor> getDynamicProperties(final ConfigurationContext context) {
        return context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());
    }

    protected DatabasePasswordProvider getDatabasePasswordProvider(final ConfigurationContext context) {
        final PropertyValue passwordSourceProperty = context.getProperty(PASSWORD_SOURCE);
        final boolean databasePasswordProviderSelected = passwordSourceProperty != null && passwordSourceProperty.isSet()
                && PASSWORD_PROVIDER.getValue().equals(passwordSourceProperty.getValue());

        if (!databasePasswordProviderSelected) {
            return null;
        }

        final PropertyValue passwordProviderProperty = context.getProperty(DB_PASSWORD_PROVIDER);
        return passwordProviderProperty == null ? null : passwordProviderProperty.asControllerService(DatabasePasswordProvider.class);
    }

    protected DatabasePasswordRequestContext buildDatabasePasswordRequestContext(final DataSourceConfiguration configuration,
                                                                                final Map<String, String> connectionProperties) {
        return DatabasePasswordRequestContext.builder()
                .jdbcUrl(configuration.getUrl())
                .driverClassName(configuration.getDriverName())
                .databaseUser(configuration.getUserName())
                .connectionProperties(connectionProperties)
                .build();
    }

    protected KerberosUser getKerberosUser(final ConfigurationContext context) {
        final KerberosUser kerberosUser;
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService == null) {
            kerberosUser = null;
        } else {
            kerberosUser = kerberosUserService.createKerberosUser();
        }

        return kerberosUser;
    }


    @Override
    public Connection getConnection() throws ProcessException {
        return getConnection(dataSource, kerberosUser);
    }

    private Connection getConnection(final BasicDataSource dataSource, final KerberosUser kerberosUser) {
        try {
            final Connection con;
            if (kerberosUser != null) {
                KerberosAction<Connection> kerberosAction = new KerberosAction<>(kerberosUser, dataSource::getConnection, getLogger());
                con = kerberosAction.execute();
            } else {
                con = dataSource.getConnection();
            }
            return con;
        } catch (final SQLException e) {
            // If using Kerberos,  attempt to re-login
            if (kerberosUser != null) {
                try {
                    getLogger().info("Error getting connection, performing Kerberos re-login");
                    kerberosUser.login();
                } catch (KerberosLoginException le) {
                    throw new ProcessException("Unable to authenticate Kerberos principal", le);
                }
            }
            throw new ProcessException(e);
        }
    }

    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     *
     * @throws SQLException if there is an error while closing open connections
     */
    @OnDisabled
    public void shutdown() throws SQLException {
        try {
            shutdown(dataSource, kerberosUser);
        } finally {
            kerberosUser = null;
            dataSource = null;
        }
    }

    private void shutdown(final BasicDataSource dataSource, final KerberosUser kerberosUser) throws SQLException {
        try {
            if (kerberosUser != null) {
                kerberosUser.logout();
            }
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }
}
