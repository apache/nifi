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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StartBootstrapCommandTest {
    @Mock
    private BootstrapCommand runBootstrapCommand;

    @Mock
    private BootstrapCommand statusBootstrapCommand;

    private StartBootstrapCommand command;

    @BeforeEach
    void setCommand() {
        command = new StartBootstrapCommand(runBootstrapCommand, statusBootstrapCommand);
    }

    @Test
    void testRunError() {
        final CommandStatus runCommandStatus = CommandStatus.ERROR;
        when(runBootstrapCommand.getCommandStatus()).thenReturn(runCommandStatus);

        command.run();

        final CommandStatus commandStatus = command.getCommandStatus();
        assertEquals(runCommandStatus, commandStatus);
    }

    @Test
    void testRunSuccessFailed() {
        final CommandStatus runCommandStatus = CommandStatus.SUCCESS;
        when(runBootstrapCommand.getCommandStatus()).thenReturn(runCommandStatus);

        final CommandStatus statusCommandStatus = CommandStatus.FAILED;
        lenient().when(statusBootstrapCommand.getCommandStatus()).thenReturn(statusCommandStatus);

        command.run();

        final CommandStatus commandStatus = command.getCommandStatus();
        assertEquals(CommandStatus.RUNNING, commandStatus);
    }
}
