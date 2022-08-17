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
package org.apache.nifi.authentication.single.user;

import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.LoginIdentityProviderInitializationContext;
import org.apache.nifi.authentication.annotation.LoginIdentityProviderContext;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.authentication.single.user.encoder.BCryptPasswordEncoder;
import org.apache.nifi.authentication.single.user.encoder.PasswordEncoder;
import org.apache.nifi.authentication.single.user.writer.LoginCredentialsWriter;
import org.apache.nifi.authentication.single.user.writer.StandardLoginCredentialsWriter;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Single User Login Identity Provider using bcrypt password encoder
 */
public class SingleUserLoginIdentityProvider implements LoginIdentityProvider {
    protected static final String USERNAME_PROPERTY = "Username";

    protected static final String PASSWORD_PROPERTY = "Password";

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUserLoginIdentityProvider.class);

    private static final Base64.Encoder RANDOM_BYTE_ENCODER = Base64.getEncoder().withoutPadding();

    private static final int RANDOM_BYTE_LENGTH = 24;

    private static final long EXPIRATION = TimeUnit.HOURS.toMillis(8);

    protected PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    private File loginIdentityProviderConfigurationFile;

    private SingleUserCredentials configuredCredentials;

    /**
     * Set NiFi Properties using method injection
     *
     * @param niFiProperties NiFi Properties
     */
    @LoginIdentityProviderContext
    public void setProperties(final NiFiProperties niFiProperties) {
        loginIdentityProviderConfigurationFile = niFiProperties.getLoginIdentityProviderConfigurationFile();
    }

    /**
     * Authenticate using Credentials comparing password and then username for verification
     *
     * @param credentials Login Credentials
     * @return Authentication Response
     * @throws InvalidLoginCredentialsException Thrown on unverified username or password
     */
    @Override
    public AuthenticationResponse authenticate(final LoginCredentials credentials) throws InvalidLoginCredentialsException {
        final String password = credentials.getPassword();
        if (isPasswordVerified(password)) {
            final String username = credentials.getUsername();
            if (isUsernameVerified(username)) {
                return new AuthenticationResponse(username, username, EXPIRATION, getClass().getSimpleName());
            } else {
                throw new InvalidLoginCredentialsException("Username verification failed");
            }
        } else {
            throw new InvalidLoginCredentialsException("Password verification failed");
        }
    }

    /**
     * Initialize Provider
     *
     * @param initializationContext Initialization Context
     */
    @Override
    public void initialize(final LoginIdentityProviderInitializationContext initializationContext) {
        LOGGER.debug("Initializing Provider");
    }

    /**
     * Configure Provider using Username and Password properties with support for generating random values
     *
     * @param configurationContext Configuration Context containing properties
     * @throws ProviderCreationException Thrown when unable to write Login Credentials
     */
    @Override
    public void onConfigured(final LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        LOGGER.debug("Configuring Provider");
        final String password = configurationContext.getProperty(PASSWORD_PROPERTY);
        if (password == null || password.length() == 0) {
            try {
                configuredCredentials = generateLoginCredentials();
                LOGGER.info("Updating Login Identity Providers Configuration [{}]", loginIdentityProviderConfigurationFile);
                final LoginCredentialsWriter loginCredentialsWriter = getLoginCredentialsWriter();
                loginCredentialsWriter.writeLoginCredentials(configuredCredentials);
            } catch (final IOException e) {
                throw new ProviderCreationException("Generating Login Credentials Failed", e);
            }
        } else {
            final String username = configurationContext.getProperty(USERNAME_PROPERTY);
            configuredCredentials = new SingleUserCredentials(username, password, getClass().getName());
        }
    }

    /**
     * Destroy Provider
     */
    @Override
    public void preDestruction() {
        LOGGER.debug("Destroying Provider");
    }

    protected String generatePassword() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] bytes = new byte[RANDOM_BYTE_LENGTH];
        secureRandom.nextBytes(bytes);
        return RANDOM_BYTE_ENCODER.encodeToString(bytes);
    }

    private LoginCredentialsWriter getLoginCredentialsWriter() {
        return new StandardLoginCredentialsWriter(loginIdentityProviderConfigurationFile);
    }

    private SingleUserCredentials generateLoginCredentials() throws IOException {
        final String username = UUID.randomUUID().toString();
        final String password = generatePassword();

        final String separator = System.lineSeparator();
        LOGGER.info("{}{}Generated Username [{}]{}Generated Password [{}]{}", separator, separator, username, separator, password, separator);
        LOGGER.info("Run the following command to change credentials: nifi.sh set-single-user-credentials USERNAME PASSWORD");

        final String hashedPassword = passwordEncoder.encode(password.toCharArray());
        return new SingleUserCredentials(username, hashedPassword, getClass().getName());
    }

    private boolean isPasswordVerified(final String password) {
        return passwordEncoder.matches(password.toCharArray(), configuredCredentials.getPassword());
    }

    private boolean isUsernameVerified(final String username) {
        return configuredCredentials.getUsername().equals(username);
    }
}
