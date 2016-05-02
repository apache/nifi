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
package org.apache.nifi.authentication.file;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.LoginIdentityProviderInitializationContext;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.authentication.exception.ProviderDestructionException;
import org.apache.nifi.util.FormatUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Identity provider for simple username/password authentication backed by a local credentials file.  Please see
 * {@link CredentialsStore} for information about the credentials file format and {@link CredentialsCLI} for
 * a sample admin utility that manipulates the credentials file.
 *
 * @see CredentialsStore
 * @see CredentialsCLI
 */
public class FileIdentityProvider implements LoginIdentityProvider {

    static final String PROPERTY_CREDENTIALS_FILE = "Credentials File";
    static final String PROPERTY_EXPIRATION_PERIOD = "Authentication Expiration";

    private static final Logger logger = LoggerFactory.getLogger(FileIdentityProvider.class);

    private String issuer;
    private long expirationPeriodMilliseconds;
    private String credentialsFilePath;
    private CredentialsStore credentialsStore;
    private String identifier;

    @Override
    public final void initialize(final LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
        this.identifier = initializationContext.getIdentifier();
        this.issuer = getClass().getSimpleName();
    }

    @Override
    public final void onConfigured(final LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final Map<String, String> configProperties = configurationContext.getProperties();
        for (String propertyKey : configProperties.keySet()) {
            String propValue = configProperties.get(propertyKey);
            logger.debug("found property '{}': '{}'", propertyKey, propValue);
        }

        credentialsFilePath = configProperties.get(PROPERTY_CREDENTIALS_FILE);
        if (StringUtils.isEmpty(credentialsFilePath)) {
            final String message = String.format("Identity Provider '%s' requires a credentials file path in property '%s'",
                    identifier, PROPERTY_CREDENTIALS_FILE);
            throw new ProviderCreationException(message);
        }
        File credentialsFile = new File(credentialsFilePath);
        if (!credentialsFile.exists()) {
            final String message = String.format("Identity Provider '%s' credentials file does not exist: '%s'",
                    identifier, credentialsFilePath);
            logger.warn(message);
        }
        credentialsStore = new CredentialsStore(credentialsFile);

        final String rawExpirationPeriod = configProperties.get(PROPERTY_EXPIRATION_PERIOD);
        if (rawExpirationPeriod == null || rawExpirationPeriod.isEmpty()) {
            final String message = String.format("Identity Provider '%s' requires a credential expiration in property '%s'",
                    identifier, PROPERTY_EXPIRATION_PERIOD);
            throw new ProviderCreationException(message);
        } else {
            try {
                expirationPeriodMilliseconds = FormatUtils.getTimeDuration(rawExpirationPeriod, TimeUnit.MILLISECONDS);
            } catch (IllegalArgumentException iae) {
                final String message = String.format("Identity Provider '%s' property '%s' value of '%s', is not a valid time period",
                        identifier, PROPERTY_EXPIRATION_PERIOD, rawExpirationPeriod);
                throw new ProviderCreationException(message);
            }
        }

        logger.debug("Identity Provider '{}' configured to use file '{}' and expiration period of '{}'={} milliseconds",
                identifier, credentialsFilePath, rawExpirationPeriod, expirationPeriodMilliseconds);
    }

    String getCredentialsFilePath() {
        return credentialsFilePath;
    }

    long getExpirationPeriod() {
        return expirationPeriodMilliseconds;
    }

    @Override
    public final AuthenticationResponse authenticate(final LoginCredentials credentials) throws InvalidLoginCredentialsException, IdentityAccessException {
        final String loginUsername = credentials.getUsername();
        final String loginPassword = credentials.getPassword();
        AuthenticationResponse authResponse = null;

        try {
            credentialsStore.reloadIfModified();
            boolean passwordMatches = credentialsStore.checkPassword(loginUsername, loginPassword);
            if (passwordMatches) {
                authResponse = new AuthenticationResponse(loginUsername, loginUsername, expirationPeriodMilliseconds,
                        issuer);
            }
        } catch (Exception ex) {
            // This message is written to the log so it can be specific and helpful
            logger.error("Identity Provider '{}' failed attempting user authentication", identifier, ex);

            // This message is shown to the user so should not be very specific
            throw new IdentityAccessException("Authentication failed", ex);
        }

        if (authResponse == null) {
            // This exception message is not shown in the UI
            throw new InvalidLoginCredentialsException("The login attempt was unsuccessful");
        }

        return authResponse;
    }

    @Override
    public final void preDestruction() throws ProviderDestructionException {
    }

}
