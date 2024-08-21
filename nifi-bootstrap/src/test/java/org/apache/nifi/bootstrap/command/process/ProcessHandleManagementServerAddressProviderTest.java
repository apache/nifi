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

import org.apache.nifi.bootstrap.configuration.SystemProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProcessHandleManagementServerAddressProviderTest {
    private static final String SYSTEM_PROPERTY_ARGUMENT = "-D%s=%s";

    private static final String ADDRESS = "127.0.0.1:52020";

    @Mock
    private ProcessHandle processHandle;

    @Mock
    private ProcessHandle.Info processHandleInfo;

    private ProcessHandleManagementServerAddressProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new ProcessHandleManagementServerAddressProvider(processHandle);
    }

    @Test
    void testGetAddressNotFound() {
        when(processHandle.info()).thenReturn(processHandleInfo);

        final Optional<String> managementServerAddress = provider.getAddress();

        assertTrue(managementServerAddress.isEmpty());
    }

    @Test
    void testGetAddressArgumentNotFound() {
        when(processHandle.info()).thenReturn(processHandleInfo);
        when(processHandleInfo.arguments()).thenReturn(Optional.empty());

        final Optional<String> managementServerAddress = provider.getAddress();

        assertTrue(managementServerAddress.isEmpty());
    }

    @Test
    void testGetAddress() {
        when(processHandle.info()).thenReturn(processHandleInfo);

        final String systemPropertyArgument = SYSTEM_PROPERTY_ARGUMENT.formatted(SystemProperty.MANAGEMENT_SERVER_ADDRESS.getProperty(), ADDRESS);
        final String[] arguments = new String[]{systemPropertyArgument};

        when(processHandleInfo.arguments()).thenReturn(Optional.of(arguments));

        final Optional<String> managementServerAddress = provider.getAddress();

        assertTrue(managementServerAddress.isPresent());

        final String address = managementServerAddress.get();
        assertEquals(ADDRESS, address);
    }
}
