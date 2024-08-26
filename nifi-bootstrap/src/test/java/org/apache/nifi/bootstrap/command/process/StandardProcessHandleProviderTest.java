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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class StandardProcessHandleProviderTest {
    @Mock
    private ConfigurationProvider configurationProvider;

    private StandardProcessHandleProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new StandardProcessHandleProvider(configurationProvider);
    }

    @Test
    void testFindApplicationProcessHandleEmpty() {
        final Optional<ProcessHandle> applicationProcessHandle = provider.findApplicationProcessHandle();

        assertTrue(applicationProcessHandle.isEmpty());
    }

    @Test
    void testFindBootstrapProcessHandleEmpty() {
        final Optional<ProcessHandle> bootstrapProcessHandle = provider.findBootstrapProcessHandle();

        assertTrue(bootstrapProcessHandle.isEmpty());
    }
}
