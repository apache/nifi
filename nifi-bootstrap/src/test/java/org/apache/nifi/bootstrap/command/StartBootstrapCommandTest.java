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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StartBootstrapCommandTest {
    private static final Duration START_STATUS_DELAY = Duration.ofMillis(25);

    private static final Duration RUN_STATUS_DELAY = Duration.ofMillis(500);

    @Mock
    private BootstrapCommand runBootstrapCommand;

    @Mock
    private BootstrapCommand statusBootstrapCommand;

    private StartBootstrapCommand command;

    @BeforeEach
    void setCommand() {
        command = new StartBootstrapCommand(runBootstrapCommand, statusBootstrapCommand, START_STATUS_DELAY);
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
    void testRunSuccessFailed() throws InterruptedException {
        final CommandStatus runCommandStatus = CommandStatus.SUCCESS;
        when(runBootstrapCommand.getCommandStatus()).thenReturn(runCommandStatus);

        final CommandStatus statusCommandStatus = CommandStatus.FAILED;
        when(statusBootstrapCommand.getCommandStatus()).thenReturn(statusCommandStatus);

        command.run();

        final CommandStatus runningStatus = command.getCommandStatus();
        assertEquals(CommandStatus.RUNNING, runningStatus);

        TimeUnit.MILLISECONDS.sleep(RUN_STATUS_DELAY.toMillis());

        final CommandStatus failedStatus = command.getCommandStatus();
        assertEquals(CommandStatus.FAILED, failedStatus);
    }
}
