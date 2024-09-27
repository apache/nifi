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
package org.apache.nifi.runtime.command;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.runtime.ManagementServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ShutdownCommandTest {
    @Mock
    private NiFiServer server;

    @Mock
    private ManagementServer managementServer;

    @Mock
    private DiagnosticsCommand diagnosticsCommand;

    private ShutdownCommand command;

    @BeforeEach
    void setCommand() {
        command = new ShutdownCommand(server, managementServer, diagnosticsCommand);
    }

    @Test
    void testRun() {
        command.run();

        assertStopped();
    }

    @Test
    void testRunDiagnosticsException() {
        doThrow(new RuntimeException()).when(diagnosticsCommand).run();

        command.run();

        assertStopped();
    }

    @Test
    void testRunManagementServerException() {
        doThrow(new RuntimeException()).when(managementServer).stop();

        command.run();

        assertStopped();
    }

    @Test
    void testRunApplicationServerException() {
        doThrow(new RuntimeException()).when(server).stop();

        command.run();

        assertStopped();
    }

    private void assertStopped() {
        verify(diagnosticsCommand).run();
        verify(managementServer).stop();
        verify(server).stop();
    }
}
