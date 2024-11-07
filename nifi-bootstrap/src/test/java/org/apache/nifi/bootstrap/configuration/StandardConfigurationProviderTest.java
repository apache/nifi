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
package org.apache.nifi.bootstrap.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardConfigurationProviderTest {

    private static final String CONFIGURATION_DIRECTORY = "conf";

    private static final String BOOTSTRAP_CONFIGURATION = "bootstrap.conf";

    private static final String CURRENT_DIRECTORY = "";

    private static final String MANAGEMENT_SERVER_ADDRESS = "http://127.0.0.1:52020";

    private final Map<String, String> environmentVariables = new LinkedHashMap<>();

    private final Properties systemProperties = new Properties();

    @BeforeEach
    void setProvider() {
        environmentVariables.clear();
        systemProperties.clear();
    }

    @Test
    void testApplicationHomeNotConfigured() {
        assertThrows(IllegalStateException.class, () -> new StandardConfigurationProvider(environmentVariables, systemProperties));
    }

    @Test
    void testGetBootstrapConfiguration(@TempDir final Path applicationHomeDirectory) throws IOException {
        environmentVariables.put(EnvironmentVariable.NIFI_HOME.name(), applicationHomeDirectory.toString());
        final Path configurationDirectory = createConfigurationDirectory(applicationHomeDirectory);
        final Path bootstrapConfiguration = setRequiredConfiguration(applicationHomeDirectory);

        final StandardConfigurationProvider provider = new StandardConfigurationProvider(environmentVariables, systemProperties);

        final Path configurationDirectoryProvided = provider.getConfigurationDirectory();
        assertEquals(configurationDirectory, configurationDirectoryProvided);

        final Path bootstrapConfigurationProvided = provider.getBootstrapConfiguration();
        assertEquals(bootstrapConfiguration, bootstrapConfigurationProvided);
    }

    @Test
    void testGetWorkingDirectory(@TempDir final Path applicationHomeDirectory) throws IOException {
        setRequiredConfiguration(applicationHomeDirectory);

        final StandardConfigurationProvider provider = new StandardConfigurationProvider(environmentVariables, systemProperties);

        final Path workingDirectory = provider.getWorkingDirectory();
        final Path workingDirectoryExpected = Paths.get(CURRENT_DIRECTORY).toAbsolutePath();

        assertEquals(workingDirectoryExpected, workingDirectory);
    }

    @Test
    void testGetManagementServerAddressNotConfigured(@TempDir final Path applicationHomeDirectory) throws IOException {
        setRequiredConfiguration(applicationHomeDirectory);

        final StandardConfigurationProvider provider = new StandardConfigurationProvider(environmentVariables, systemProperties);

        final Optional<URI> managementServerAddress = provider.getManagementServerAddress();

        assertTrue(managementServerAddress.isEmpty());
    }

    @Test
    void testGetManagementServerAddress(@TempDir final Path applicationHomeDirectory) throws IOException {
        final Path bootstrapConfiguration = setRequiredConfiguration(applicationHomeDirectory);

        final Properties bootstrapProperties = new Properties();
        bootstrapProperties.put(BootstrapProperty.MANAGEMENT_SERVER_ADDRESS.getProperty(), MANAGEMENT_SERVER_ADDRESS);
        try (OutputStream outputStream = Files.newOutputStream(bootstrapConfiguration)) {
            bootstrapProperties.store(outputStream, Properties.class.getSimpleName());
        }

        final StandardConfigurationProvider provider = new StandardConfigurationProvider(environmentVariables, systemProperties);

        final Optional<URI> managementServerAddress = provider.getManagementServerAddress();

        assertTrue(managementServerAddress.isPresent());
        final URI address = managementServerAddress.get();
        assertEquals(MANAGEMENT_SERVER_ADDRESS, address.toString());
    }

    @Test
    void testGetAdditionalArguments(@TempDir final Path applicationHomeDirectory) throws IOException {
        final Path bootstrapConfiguration = setRequiredConfiguration(applicationHomeDirectory);
        // Properties in random order and containing some java.arg and some non-java.arg names.
        List<String> propertyNames = List.of("java.arg9", "java.arg2", "java.arg.my2", "non.java.arg.2",
                "java.arg1", "java.arg.memory", "java.arg", "java.arg.my1", "non.java.arg.3", "random.nothing");
        // The expected returned list of java.arg properties sorted in ascending alphabetical order.
        List<String> expectedArguments = List.of("java.arg", "java.arg.memory", "java.arg.my1", "java.arg.my2",
                "java.arg1", "java.arg2", "java.arg9");

        final Properties bootstrapProperties = new Properties();
        for (String propertyName : propertyNames) {
            bootstrapProperties.put(propertyName, propertyName);
        }
        try (OutputStream outputStream = Files.newOutputStream(bootstrapConfiguration)) {
            bootstrapProperties.store(outputStream, Properties.class.getSimpleName());
        }

        final StandardConfigurationProvider provider = new StandardConfigurationProvider(environmentVariables, systemProperties);

        final List<String> actualAdditionalArguments = provider.getAdditionalArguments();

        assertEquals(expectedArguments.size(), actualAdditionalArguments.size());
        for (int i = 0; i < expectedArguments.size(); i++) {
            assertEquals(expectedArguments.get(i), actualAdditionalArguments.get(i));
        }
    }

    private Path setRequiredConfiguration(final Path applicationHomeDirectory) throws IOException {
        environmentVariables.put(EnvironmentVariable.NIFI_HOME.name(), applicationHomeDirectory.toString());
        final Path configurationDirectory = createConfigurationDirectory(applicationHomeDirectory);
        return createBootstrapConfiguration(configurationDirectory);
    }

    private Path createConfigurationDirectory(final Path applicationHomeDirectory) {
        final Path configurationDirectory = applicationHomeDirectory.resolve(CONFIGURATION_DIRECTORY);
        if (configurationDirectory.toFile().mkdir()) {
            assertTrue(Files.isReadable(configurationDirectory));
        }
        return configurationDirectory;
    }

    private Path createBootstrapConfiguration(final Path configurationDirectory) throws IOException {
        final Path bootstrapConfiguration = configurationDirectory.resolve(BOOTSTRAP_CONFIGURATION);
        assertTrue(bootstrapConfiguration.toFile().createNewFile());
        return bootstrapConfiguration;
    }
}
