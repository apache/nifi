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
package org.apache.nifi.bootstrap.command.process;

import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.configuration.SystemProperty;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

/**
 * Standard implementation of Process Builder Provider for constructing application command arguments
 */
public class StandardProcessBuilderProvider implements ProcessBuilderProvider {
    private static final String JAR_FILE_EXTENSION = ".jar";

    private static final BiPredicate<Path, BasicFileAttributes> JAR_FILE_MATCHER = (path, attributes) -> path.getFileName().toString().endsWith(JAR_FILE_EXTENSION);

    private static final int LIBRARY_JAR_DEPTH = 1;

    private static final String SYSTEM_PROPERTY = "-D%s=%s";

    private static final String CLASS_PATH_ARGUMENT = "--class-path";

    private final ConfigurationProvider configurationProvider;

    private final ManagementServerAddressProvider managementServerAddressProvider;

    public StandardProcessBuilderProvider(final ConfigurationProvider configurationProvider, final ManagementServerAddressProvider managementServerAddressProvider) {
        this.configurationProvider = Objects.requireNonNull(configurationProvider);
        this.managementServerAddressProvider = Objects.requireNonNull(managementServerAddressProvider);
    }

    @Override
    public ProcessBuilder getApplicationProcessBuilder() {
        final ProcessBuilder processBuilder = new ProcessBuilder();

        final List<String> command = getCommand();
        processBuilder.command(command);

        return processBuilder;
    }

    private List<String> getCommand() {
        final List<String> command = new ArrayList<>();

        final ProcessHandle.Info currentProcessHandleInfo = ProcessHandle.current().info();
        final String currentProcessCommand = getCurrentProcessCommand(currentProcessHandleInfo);
        command.add(currentProcessCommand);

        final String classPath = getClassPath();
        command.add(CLASS_PATH_ARGUMENT);
        command.add(classPath);

        final Path logDirectory = configurationProvider.getLogDirectory();
        final String logDirectoryProperty = SYSTEM_PROPERTY.formatted(SystemProperty.LOG_DIRECTORY.getProperty(), logDirectory);
        command.add(logDirectoryProperty);

        final Path applicationProperties = configurationProvider.getApplicationProperties();
        final String applicationPropertiesProperty = SYSTEM_PROPERTY.formatted(SystemProperty.APPLICATION_PROPERTIES.getProperty(), applicationProperties);
        command.add(applicationPropertiesProperty);

        final String managementServerAddress = managementServerAddressProvider.getAddress().orElseThrow(() -> new IllegalStateException("Management Server Address not configured"));
        final String managementServerAddressProperty = SYSTEM_PROPERTY.formatted(SystemProperty.MANAGEMENT_SERVER_ADDRESS.getProperty(), managementServerAddress);
        command.add(managementServerAddressProperty);

        final List<String> additionalArguments = configurationProvider.getAdditionalArguments();
        command.addAll(additionalArguments);

        command.add(ApplicationClassName.APPLICATION.getName());
        return command;
    }

    private String getCurrentProcessCommand(final ProcessHandle.Info currentProcessHandleInfo) {
        final Optional<String> currentProcessHandleCommand = currentProcessHandleInfo.command();
        return currentProcessHandleCommand.orElseThrow(IllegalStateException::new);
    }

    private String getClassPath() {
        final Path libraryDirectory = configurationProvider.getLibraryDirectory();
        try (
                Stream<Path> libraryFiles = Files.find(libraryDirectory, LIBRARY_JAR_DEPTH, JAR_FILE_MATCHER)
        ) {
            final List<String> libraryPaths = new ArrayList<>(libraryFiles.map(Path::toString).toList());

            final Path configurationDirectory = configurationProvider.getConfigurationDirectory();
            libraryPaths.add(configurationDirectory.toString());

            return String.join(File.pathSeparator, libraryPaths);
        } catch (final IOException e) {
            throw new IllegalStateException("Read Library Directory [%s] failed".formatted(libraryDirectory), e);
        }
    }
}
