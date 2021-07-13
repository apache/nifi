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
package org.apache.nifi.authentication.single.user.command;

import org.apache.nifi.authentication.single.user.SingleUserCredentials;
import org.apache.nifi.authentication.single.user.encoder.BCryptPasswordEncoder;
import org.apache.nifi.authentication.single.user.encoder.PasswordEncoder;
import org.apache.nifi.authentication.single.user.writer.LoginCredentialsWriter;
import org.apache.nifi.authentication.single.user.writer.StandardLoginCredentialsWriter;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

/**
 * Set Single User Credentials in Login Identity Providers configuration
 */
public class SetSingleUserCredentials {
    protected static final String PROPERTIES_FILE_PATH = "nifi.properties.file.path";

    protected static final String PROVIDERS_PROPERTY = "nifi.login.identity.provider.configuration.file";

    private static final String PROVIDER_CLASS = "org.apache.nifi.authentication.single.user.SingleUserLoginIdentityProvider";

    private static final PasswordEncoder PASSWORD_ENCODER = new BCryptPasswordEncoder();

    private static final int MINIMUM_USERNAME_LENGTH = 4;

    private static final int MINIMUM_PASSWORD_LENGTH = 12;

    public static void main(final String[] arguments) {
        if (arguments.length == 2) {
            final String username = arguments[0];
            final String password = arguments[1];

            if (username.length() < MINIMUM_USERNAME_LENGTH) {
                System.err.printf("ERROR: Username must be at least %d characters%n", MINIMUM_USERNAME_LENGTH);
            } else if (password.length() < MINIMUM_PASSWORD_LENGTH) {
                System.err.printf("ERROR: Password must be at least %d characters%n", MINIMUM_PASSWORD_LENGTH);
            } else {
                final String encodedPassword = PASSWORD_ENCODER.encode(password.toCharArray());
                run(username, encodedPassword);
            }
        } else {
            System.err.printf("Unexpected number of arguments [%d]%n", arguments.length);
            System.err.printf("Usage: %s <username> <password>%n", SetSingleUserCredentials.class.getSimpleName());
        }
    }

    private static void run(final String username, final String encodedPassword) {
        final String propertiesFilePath = System.getProperty(PROPERTIES_FILE_PATH);
        final File propertiesFile = new File(propertiesFilePath);
        final Properties properties = loadProperties(propertiesFile);

        final SingleUserCredentials credentials = new SingleUserCredentials(username, encodedPassword, PROVIDER_CLASS);
        final File providersFile = getLoginIdentityProvidersFile(properties);
        setCredentials(credentials, providersFile);
        System.out.printf("Login Identity Providers Processed [%s]%n", providersFile.getAbsolutePath());
    }

    private static void setCredentials(final SingleUserCredentials singleUserCredentials, final File providersFile) {
        final LoginCredentialsWriter writer = new StandardLoginCredentialsWriter(providersFile);
        writer.writeLoginCredentials(singleUserCredentials);
    }

    private static Properties loadProperties(final File propertiesFile) {
        final Properties properties = new Properties();
        try (final FileReader reader = new FileReader(propertiesFile)) {
            properties.load(reader);
        } catch (final IOException e) {
            final String message = String.format("Failed to read NiFi Properties [%s]", propertiesFile);
            throw new UncheckedIOException(message, e);
        }
        return properties;
    }

    private static File getLoginIdentityProvidersFile(final Properties properties) {
        return new File(properties.getProperty(PROVIDERS_PROPERTY));
    }
}
