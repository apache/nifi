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
package org.apache.nifi.bootstrap.command;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ApplicationProcessStatusBootstrapCommandTest {
    @Mock
    private ProcessHandle processHandle;

    @Mock
    private ProcessHandle applicationProcessHandle;

    private ApplicationProcessStatusBootstrapCommand command;

    @BeforeEach
    void setCommand() {
        command = new ApplicationProcessStatusBootstrapCommand(processHandle);
    }

    @Test
    void testRunStopped() {
        when(processHandle.children()).thenReturn(Stream.empty());

        command.run();
        final CommandStatus commandStatus = command.getCommandStatus();

        assertEquals(CommandStatus.STOPPED, commandStatus);
    }

    @Test
    void testRunSuccess() {
        when(processHandle.children()).thenReturn(Stream.of(applicationProcessHandle));
        when(applicationProcessHandle.isAlive()).thenReturn(true);

        command.run();
        final CommandStatus commandStatus = command.getCommandStatus();

        assertEquals(CommandStatus.SUCCESS, commandStatus);
    }

    @Test
    void testRunCommunicationFailed() {
        when(processHandle.children()).thenReturn(Stream.of(applicationProcessHandle));
        when(applicationProcessHandle.isAlive()).thenReturn(false);

        command.run();
        final CommandStatus commandStatus = command.getCommandStatus();

        assertEquals(CommandStatus.COMMUNICATION_FAILED, commandStatus);
    }
}
