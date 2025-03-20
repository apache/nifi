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
import org.apache.nifi.bootstrap.configuration.SystemProperty;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Provider implementation resolves the Management Server Address from System Properties of Application Virtual Machine using the Attach API
 */
public class VirtualMachineManagementServerAddressProvider implements ManagementServerAddressProvider {
    private final ProcessHandle processHandle;

    public VirtualMachineManagementServerAddressProvider(final ProcessHandle processHandle) {
        this.processHandle = Objects.requireNonNull(processHandle);
    }

    /**
     * Get Management Server Address from System Properties of Application Virtual Machine using Attach API
     *
     * @return Management Server Address or null when not found
     */
    @Override
    public Optional<String> getAddress() {
        String managementServerAddress = null;

        final String applicationProcessId = Long.toString(processHandle.pid());

        try {
            final VirtualMachine virtualMachine = VirtualMachine.attach(applicationProcessId);
            try {
                managementServerAddress = getAddress(virtualMachine);
            } finally {
                virtualMachine.detach();
            }
        } catch (final Exception ignored) {

        }

        return Optional.ofNullable(managementServerAddress);
    }

    private String getAddress(final VirtualMachine virtualMachine) throws IOException {
        final Properties systemProperties = virtualMachine.getSystemProperties();
        return systemProperties.getProperty(SystemProperty.MANAGEMENT_SERVER_ADDRESS.getProperty());
    }
}
