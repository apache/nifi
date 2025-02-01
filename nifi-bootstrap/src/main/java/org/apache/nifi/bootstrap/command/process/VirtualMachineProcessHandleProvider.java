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

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;
import com.sun.tools.attach.spi.AttachProvider;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.configuration.SystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Virtual Machine implementation of ProcessHandle Provider using the Attach API with System Properties
 */
public class VirtualMachineProcessHandleProvider implements ProcessHandleProvider {
    private static final Logger logger = LoggerFactory.getLogger(VirtualMachineProcessHandleProvider.class);

    private final ConfigurationProvider configurationProvider;

    public VirtualMachineProcessHandleProvider(final ConfigurationProvider configurationProvider) {
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
        final ProcessHandle currentProcessHandle = ProcessHandle.current();
        final String currentProcessId = Long.toString(currentProcessHandle.pid());

        Optional<ProcessHandle> processHandleFound = Optional.empty();

        final List<VirtualMachineDescriptor> virtualMachineDescriptors = VirtualMachine.list();
        for (final VirtualMachineDescriptor virtualMachineDescriptor : virtualMachineDescriptors) {
            final String virtualMachineId = virtualMachineDescriptor.id();
            if (currentProcessId.equals(virtualMachineId)) {
                continue;
            }

            processHandleFound = findProcessHandle(virtualMachineDescriptor, systemProperty, configuration);
            if (processHandleFound.isPresent()) {
                break;
            }
        }

        return processHandleFound;
    }

    private Optional<ProcessHandle> findProcessHandle(final VirtualMachineDescriptor descriptor, final SystemProperty systemProperty, final Path configuration) {
        final AttachProvider attachProvider = descriptor.provider();
        final String virtualMachineId = descriptor.id();

        Optional<ProcessHandle> processHandle = Optional.empty();
        try {
            final VirtualMachine virtualMachine = attachProvider.attachVirtualMachine(virtualMachineId);
            logger.debug("Attached Virtual Machine [{}]", virtualMachine.id());
            try {
                processHandle = findProcessHandle(virtualMachine, systemProperty, configuration);
            } finally {
                virtualMachine.detach();
            }
        } catch (final Exception e) {
            logger.debug("Attach Virtual Machine [{}] failed", virtualMachineId, e);
        }

        return processHandle;
    }

    private Optional<ProcessHandle> findProcessHandle(final VirtualMachine virtualMachine, final SystemProperty systemProperty, final Path configuration) throws IOException {
        final Properties systemProperties = virtualMachine.getSystemProperties();
        final String configurationProperty = systemProperties.getProperty(systemProperty.getProperty());
        final String configurationPath = configuration.toString();

        final Optional<ProcessHandle> processHandle;

        if (configurationPath.equals(configurationProperty)) {
            final String virtualMachineId = virtualMachine.id();
            final long processId = Long.parseLong(virtualMachineId);
            processHandle = ProcessHandle.of(processId);
        } else {
            processHandle = Optional.empty();
        }

        return processHandle;
    }
}
