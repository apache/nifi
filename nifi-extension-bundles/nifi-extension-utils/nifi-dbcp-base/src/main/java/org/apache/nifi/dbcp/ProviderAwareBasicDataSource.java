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
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.Constants;
import org.apache.commons.dbcp2.DriverConnectionFactory;
import org.apache.nifi.dbcp.api.DatabaseCredentialPlacement;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * Extension of {@link BasicDataSource} that supports obtaining database passwords from a {@link DatabasePasswordProvider}.
 */
public class ProviderAwareBasicDataSource extends BasicDataSource {
    private static final String USER_PROPERTY = "user";
    private static final String USER_NAME_PROPERTY = "username";
    private static final String ACCESS_TOKEN_PROPERTY = "accessToken";

    private volatile DatabasePasswordProvider databasePasswordProvider;
    private volatile DatabasePasswordRequestContext passwordRequestContext;

    public void setDatabasePasswordProvider(final DatabasePasswordProvider passwordProvider,
                                            final DatabasePasswordRequestContext requestContext) {
        if (passwordProvider != null && requestContext == null) {
            throw new IllegalArgumentException("Database Password Request Context required when a provider is configured");
        }
        this.databasePasswordProvider = passwordProvider;
        this.passwordRequestContext = requestContext;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws SQLException {
        final ConnectionFactory delegate = super.createConnectionFactory();

        if (databasePasswordProvider == null) {
            return delegate;
        }

        if (delegate instanceof final DriverConnectionFactory driverConnectionFactory) {
            return new PasswordRefreshingConnectionFactory(driverConnectionFactory, databasePasswordProvider, passwordRequestContext);
        }

        throw new SQLException("Database Password Provider configured but unsupported ConnectionFactory [%s]".formatted(
                delegate.getClass().getName()));
    }

    private static class PasswordRefreshingConnectionFactory extends DriverConnectionFactory {
        private final DatabasePasswordProvider passwordProvider;
        private final DatabasePasswordRequestContext passwordRequestContext;

        PasswordRefreshingConnectionFactory(final DriverConnectionFactory delegate,
                                            final DatabasePasswordProvider passwordProvider,
                                            final DatabasePasswordRequestContext passwordRequestContext) {
            super(delegate.getDriver(), delegate.getConnectionString(), delegate.getProperties());
            this.passwordProvider = passwordProvider;
            this.passwordRequestContext = passwordRequestContext;
        }

        @Override
        public Connection createConnection() throws SQLException {
            final DatabaseCredentialPlacement credentialPlacement = passwordProvider.getDatabaseCredentialPlacement();
            final char[] passwordCharacters;
            try {
                passwordCharacters = passwordProvider.getPassword(passwordRequestContext);
            } catch (final Exception e) {
                throw new SQLException("Failed to obtain database password from provider", e);
            }

            if (passwordCharacters == null || passwordCharacters.length == 0) {
                throw new SQLException("Database Password Provider returned an empty password");
            }

            final String credentialValue;
            try {
                credentialValue = new String(passwordCharacters);
            } finally {
                Arrays.fill(passwordCharacters, '\0');
            }

            final Properties connectionProperties = copyConnectionProperties();
            final String credentialPropertyName = configureCredentialPlacement(connectionProperties, credentialPlacement, credentialValue);
            try {
                return getDriver().connect(getConnectionString(), connectionProperties);
            } finally {
                connectionProperties.remove(credentialPropertyName);
            }
        }

        private Properties copyConnectionProperties() {
            final Properties connectionProperties = new Properties();
            connectionProperties.putAll(getProperties());
            return connectionProperties;
        }

        private String configureCredentialPlacement(final Properties connectionProperties,
                                                   final DatabaseCredentialPlacement credentialPlacement,
                                                   final String credentialValue) {
            if (credentialPlacement == DatabaseCredentialPlacement.ACCESS_TOKEN) {
                removeAccessTokenConflicts(connectionProperties);
                connectionProperties.put(USER_PROPERTY, "");
                connectionProperties.put(Constants.KEY_PASSWORD, "");
                connectionProperties.put(ACCESS_TOKEN_PROPERTY, credentialValue);
                return ACCESS_TOKEN_PROPERTY;
            }

            connectionProperties.put(Constants.KEY_PASSWORD, credentialValue);
            return Constants.KEY_PASSWORD;
        }

        private void removeAccessTokenConflicts(final Properties connectionProperties) {
            connectionProperties.keySet().removeIf(key -> {
                if (!(key instanceof final String propertyName)) {
                    return false;
                }

                final String normalizedPropertyName = propertyName.toLowerCase(Locale.ROOT);
                return USER_PROPERTY.equals(normalizedPropertyName)
                        || USER_NAME_PROPERTY.equals(normalizedPropertyName)
                        || Constants.KEY_PASSWORD.equals(normalizedPropertyName);
            });
        }
    }
}
