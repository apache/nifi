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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VirtualMachineProcessHandleProviderTest {
    @Mock
    private ConfigurationProvider configurationProvider;

    @TempDir
    private Path tempDir;

    private VirtualMachineProcessHandleProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new VirtualMachineProcessHandleProvider(configurationProvider);
    }

    @Test
    void testFindApplicationProcessHandleEmpty() {
        when(configurationProvider.getApplicationProperties()).thenReturn(tempDir);
        final Optional<ProcessHandle> applicationProcessHandle = provider.findApplicationProcessHandle();

        assertTrue(applicationProcessHandle.isEmpty());
    }

    @Test
    void testFindBootstrapProcessHandleEmpty() {
        when(configurationProvider.getBootstrapConfiguration()).thenReturn(tempDir);
        final Optional<ProcessHandle> bootstrapProcessHandle = provider.findBootstrapProcessHandle();

        assertTrue(bootstrapProcessHandle.isEmpty());
    }
}
