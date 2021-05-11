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

import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class SetSingleUserCredentialsTest {
    private static final Pattern BCRYPT_PATTERN = Pattern.compile("\\$2b\\$12\\$.+");

    private static final String PROPERTIES_PATH = "/conf/login.nifi.properties";

    @After
    public void clearProperties() {
        System.clearProperty(SetSingleUserCredentials.PROPERTIES_FILE_PATH);
    }

    @Test
    public void testMainNoArguments() {
        SetSingleUserCredentials.main(new String[]{});
    }

    @Test
    public void testMainUsernamePasswordUpdated() throws IOException, URISyntaxException {
        final Path providersConfiguration = getProvidersConfiguration();
        final Path propertiesPath = getNiFiProperties(providersConfiguration);
        System.setProperty(SetSingleUserCredentials.PROPERTIES_FILE_PATH, propertiesPath.toString());

        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        SetSingleUserCredentials.main(new String[]{username, password});
        assertProvidersUpdated(providersConfiguration, username);
    }

    private void assertProvidersUpdated(final Path providersConfigurationPath, final String username) throws IOException {
        final String providersConfiguration = new String(Files.readAllBytes(providersConfigurationPath), StandardCharsets.UTF_8);

        assertTrue("Username not found", providersConfiguration.contains(username));
        assertTrue("Encoded Password not found", BCRYPT_PATTERN.matcher(providersConfiguration).find());
    }

    private Path getNiFiProperties(final Path providersPath) throws IOException, URISyntaxException {
        final Path sourcePropertiesPath = Paths.get(SetSingleUserCredentials.class.getResource(PROPERTIES_PATH).toURI());
        final List<String> sourceProperties = Files.readAllLines(sourcePropertiesPath);
        final List<String> flowProperties = sourceProperties.stream().map(line -> {
            if (line.startsWith(SetSingleUserCredentials.PROVIDERS_PROPERTY)) {
                final String providersPathProperty = FilenameUtils.separatorsToUnix(providersPath.toString());
                return String.format("%s=%s", SetSingleUserCredentials.PROVIDERS_PROPERTY, providersPathProperty);
            } else {
                return line;
            }
        }).collect(Collectors.toList());

        final Path propertiesPath = Files.createTempFile(SetSingleUserCredentials.class.getSimpleName(), ".properties");
        propertiesPath.toFile().deleteOnExit();
        Files.write(propertiesPath, flowProperties);
        return propertiesPath;
    }

    private Path getProvidersConfiguration() throws IOException {
        final Path providersConfigurationPath = Files.createTempFile(SetSingleUserCredentials.class.getSimpleName(), ".xml");
        final File providersConfigurationFile = providersConfigurationPath.toFile();
        providersConfigurationFile.deleteOnExit();

        try (final InputStream inputStream = SetSingleUserCredentials.class.getResourceAsStream("/conf/standard-login-identity-providers.xml")) {
            Files.copy(inputStream, providersConfigurationPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return providersConfigurationPath;
    }
}
