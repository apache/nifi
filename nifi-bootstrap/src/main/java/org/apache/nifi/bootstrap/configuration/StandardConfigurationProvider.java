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

import static java.util.function.Predicate.not;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Standard implementation of Configuration Provider based on NIFI_HOME environment variable base directory
 */
public class StandardConfigurationProvider implements ConfigurationProvider {

    private static final String CONFIGURATION_DIRECTORY = "conf";

    private static final String LIBRARY_DIRECTORY = "lib";

    private static final String LOG_DIRECTORY = "logs";

    private static final String APPLICATION_PROPERTIES = "nifi.properties";

    private static final String BOOTSTRAP_CONFIGURATION = "bootstrap.conf";

    private static final String CURRENT_DIRECTORY = "";

    private static final Duration GRACEFUL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(20);

    private final Map<String, String> environmentVariables;

    private final Properties systemProperties;

    private final Properties bootstrapProperties = new Properties();

    public StandardConfigurationProvider(final Map<String, String> environmentVariables, final Properties systemProperties) {
        this.environmentVariables = Objects.requireNonNull(environmentVariables);
        this.systemProperties = Objects.requireNonNull(systemProperties);
        setBootstrapProperties();
    }

    /**
     * Get additional arguments for application command from Bootstrap Properties starting with java.arg
     * Return the list sorted by java.arg names in ascending alphabetical order
     *
     * @return Additional arguments
     */
    @Override
    public List<String> getAdditionalArguments() {
        return bootstrapProperties.stringPropertyNames().stream()
                .filter(name -> name.startsWith(BootstrapProperty.JAVA_ARGUMENT.getProperty()))
                .sorted()
                .map(bootstrapProperties::getProperty)
                .filter(not(String::isBlank))
                .toList();
    }

    /**
     * Get Application Properties relative to configuration directory
     *
     * @return Application Properties
     */
    @Override
    public Path getApplicationProperties() {
        final Path configurationDirectory = getConfigurationDirectory();
        final Path applicationProperties = configurationDirectory.resolve(APPLICATION_PROPERTIES);

        if (Files.notExists(applicationProperties)) {
            throw new IllegalStateException("Application Properties [%s] not found".formatted(applicationProperties));
        }

        return applicationProperties;
    }

    /**
     * Get Bootstrap Configuration from either System Property or relative to configuration directory
     *
     * @return Bootstrap Configuration
     */
    @Override
    public Path getBootstrapConfiguration() {
        final Path bootstrapConfiguration;

        final String bootstrapConfigurationProperty = System.getProperty(SystemProperty.BOOTSTRAP_CONFIGURATION.getProperty());
        if (isEmpty(bootstrapConfigurationProperty)) {
            final Path configurationDirectory = getConfigurationDirectory();
            bootstrapConfiguration = configurationDirectory.resolve(BOOTSTRAP_CONFIGURATION);
        } else {
            bootstrapConfiguration = Paths.get(bootstrapConfigurationProperty).toAbsolutePath();
        }

        if (Files.notExists(bootstrapConfiguration)) {
            throw new IllegalStateException("Bootstrap Configuration [%s] not found".formatted(bootstrapConfiguration));
        }

        return bootstrapConfiguration;
    }

    /**
     * Get Library Directory from Bootstrap Configuration or relative to configuration directory
     *
     * @return Library Directory
     */
    @Override
    public Path getLibraryDirectory() {
        final Path libraryDirectory = getResolvedDirectory(BootstrapProperty.LIBRARY_DIRECTORY, LIBRARY_DIRECTORY);

        if (Files.notExists(libraryDirectory)) {
            throw new IllegalStateException("Library Directory [%s] not found".formatted(libraryDirectory));
        }

        return libraryDirectory;
    }

    /**
     * Get Log Directory from System Property or relative to application home directory
     *
     * @return Log Directory
     */
    @Override
    public Path getLogDirectory() {
        final Path logDirectory;

        final String logDirectoryProperty = systemProperties.getProperty(SystemProperty.LOG_DIRECTORY.getProperty());

        if (isEmpty(logDirectoryProperty)) {
            final Path applicationHome = getApplicationHome();
            logDirectory = applicationHome.resolve(LOG_DIRECTORY);
        } else {
            logDirectory = Paths.get(logDirectoryProperty);
        }

        return logDirectory;
    }

    /**
     * Get timeout configured for graceful shutdown of application process
     *
     * @return Graceful Shutdown Timeout duration
     */
    @Override
    public Duration getGracefulShutdownTimeout() {
        final Duration gracefulShutdownTimeout;

        final String gracefulShutdownSecondsProperty = bootstrapProperties.getProperty(BootstrapProperty.GRACEFUL_SHUTDOWN_SECONDS.getProperty());
        if (gracefulShutdownSecondsProperty == null || gracefulShutdownSecondsProperty.isEmpty()) {
            gracefulShutdownTimeout = GRACEFUL_SHUTDOWN_TIMEOUT;
        } else {
            final int gracefulShutdownSeconds = Integer.parseInt(gracefulShutdownSecondsProperty);
            gracefulShutdownTimeout = Duration.ofSeconds(gracefulShutdownSeconds);
        }

        return gracefulShutdownTimeout;
    }

    /**
     * Get Management Server Address from the bootstrap configuration
     *
     * @return Management Server Address or empty when not configured
     */
    @Override
    public Optional<URI> getManagementServerAddress() {
        final Optional<URI> managementServerAddress;

        final String managementServerAddressProperty = bootstrapProperties.getProperty(BootstrapProperty.MANAGEMENT_SERVER_ADDRESS.getProperty());
        if (managementServerAddressProperty == null || managementServerAddressProperty.isEmpty()) {
            managementServerAddress = Optional.empty();
        } else {
            final URI serverAddress = URI.create(managementServerAddressProperty);
            managementServerAddress = Optional.of(serverAddress);
        }

        return managementServerAddress;
    }

    /**
     * Get Configuration Directory from Bootstrap Configuration or relative to application home directory
     *
     * @return Configuration Directory
     */
    @Override
    public Path getConfigurationDirectory() {
        final Path configurationDirectory = getResolvedDirectory(BootstrapProperty.CONFIGURATION_DIRECTORY, CONFIGURATION_DIRECTORY);

        if (Files.notExists(configurationDirectory)) {
            throw new IllegalStateException("Configuration Directory [%s] not found".formatted(configurationDirectory));
        }

        return configurationDirectory;
    }

    /**
     * Get Working Directory from Bootstrap Configuration or current working directory
     *
     * @return Working Directory
     */
    @Override
    public Path getWorkingDirectory() {
        final Path workingDirectory;

        final String workingDirectoryProperty = bootstrapProperties.getProperty(BootstrapProperty.WORKING_DIRECTORY.getProperty());
        if (isEmpty(workingDirectoryProperty)) {
            workingDirectory = Paths.get(CURRENT_DIRECTORY).toAbsolutePath();
        } else {
            workingDirectory = Paths.get(workingDirectoryProperty).toAbsolutePath();
        }

        return workingDirectory;
    }

    private Path getResolvedDirectory(final BootstrapProperty bootstrapProperty, final String relativeDirectory) {
        final Path resolvedDirectory;

        final String directoryProperty = bootstrapProperties.getProperty(bootstrapProperty.getProperty());
        if (isEmpty(directoryProperty)) {
            final Path applicationHome = getApplicationHome();
            resolvedDirectory = applicationHome.resolve(relativeDirectory);
        } else {
            final Path directoryPropertyResolved = Paths.get(directoryProperty);
            if (directoryPropertyResolved.isAbsolute()) {
                resolvedDirectory = directoryPropertyResolved;
            } else {
                final Path workingDirectory = getWorkingDirectory();
                resolvedDirectory = workingDirectory.resolve(directoryPropertyResolved);
            }
        }

        // Normalize Path removing relative directory elements
        return resolvedDirectory.normalize();
    }

    private Path getApplicationHome() {
        final Path applicationHome;

        final String applicationHomeVariable = environmentVariables.get(EnvironmentVariable.NIFI_HOME.name());
        if (isEmpty(applicationHomeVariable)) {
            throw new IllegalStateException("Application Home Environment Variable [NIFI_HOME] not configured");
        } else {
            applicationHome = Paths.get(applicationHomeVariable).toAbsolutePath();
        }

        if (Files.notExists(applicationHome)) {
            throw new IllegalStateException("Application Home [%s] not found".formatted(applicationHome));
        }

        return applicationHome;
    }

    private boolean isEmpty(final String property) {
        return property == null || property.isEmpty();
    }

    private void setBootstrapProperties() {
        final Path bootstrapConfiguration = getBootstrapConfiguration();

        try (InputStream inputStream = Files.newInputStream(bootstrapConfiguration)) {
            bootstrapProperties.load(inputStream);
        } catch (final IOException e) {
            throw new UncheckedIOException("Bootstrap Properties [%s] loading failed".formatted(bootstrapConfiguration), e);
        }
    }
}
