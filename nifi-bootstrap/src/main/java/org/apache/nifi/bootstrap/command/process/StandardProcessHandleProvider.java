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

import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.configuration.SystemProperty;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Process Handle Provider searches running Processes and locates the first handles match based on command arguments
 */
public class StandardProcessHandleProvider implements ProcessHandleProvider {

    private static final String PROPERTIES_ARGUMENT = "-D%s=%s";

    private final ConfigurationProvider configurationProvider;

    public StandardProcessHandleProvider(final ConfigurationProvider configurationProvider) {
        this.configurationProvider = Objects.requireNonNull(configurationProvider);
    }

    /**
     * Find Process Handle for Application based on matching argument for path to application properties
     *
     * @return Application Process Handle or empty when not found
     */
    @Override
    public Optional<ProcessHandle> findApplicationProcessHandle() {
        final Path applicationProperties = configurationProvider.getApplicationProperties();
        return findProcessHandle(SystemProperty.APPLICATION_PROPERTIES, applicationProperties);
    }

    /**
     * Find Process Handle for Bootstrap based on matching argument for path to bootstrap configuration
     *
     * @return Bootstrap Process Handle or empty when not found
     */
    @Override
    public Optional<ProcessHandle> findBootstrapProcessHandle() {
        final Path bootstrapConfiguration = configurationProvider.getBootstrapConfiguration();
        return findProcessHandle(SystemProperty.BOOTSTRAP_CONFIGURATION, bootstrapConfiguration);
    }

    private Optional<ProcessHandle> findProcessHandle(final SystemProperty systemProperty, final Path configuration) {
        final String propertiesArgument = PROPERTIES_ARGUMENT.formatted(systemProperty.getProperty(), configuration);
        final ProcessHandle currentProcessHandle = ProcessHandle.current();

        return ProcessHandle.allProcesses()
                .filter(Predicate.not(currentProcessHandle::equals))
                .filter(processHandle -> {
                    final ProcessHandle.Info processHandleInfo = processHandle.info();
                    final Optional<String[]> processArguments = processHandleInfo.arguments();
                    final boolean matched;
                    if (processArguments.isPresent()) {
                        final String[] arguments = processArguments.get();
                        matched = Arrays.stream(arguments).anyMatch(propertiesArgument::contentEquals);
                    } else {
                        matched = false;
                    }
                    return matched;
                })
                .findFirst();
    }
}
