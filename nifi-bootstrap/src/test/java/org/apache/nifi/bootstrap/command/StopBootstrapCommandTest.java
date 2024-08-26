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

import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StopBootstrapCommandTest {
    @Mock
    private ConfigurationProvider configurationProvider;

    @Mock
    private ProcessHandleProvider processHandleProvider;

    @Mock
    private ProcessHandle applicationProcessHandle;

    @Mock
    private ProcessHandle.Info applicationProcessHandleInfo;

    private StopBootstrapCommand command;

    @BeforeEach
    void setCommand() {
        command = new StopBootstrapCommand(processHandleProvider, configurationProvider);
    }

    @Test
    void testRunProcessHandleNotFound() {
        when(processHandleProvider.findApplicationProcessHandle()).thenReturn(Optional.empty());

        command.run();
        final CommandStatus commandStatus = command.getCommandStatus();

        assertEquals(CommandStatus.SUCCESS, commandStatus);
    }

    @Test
    void testRunDestroyCompleted() {
        when(processHandleProvider.findApplicationProcessHandle()).thenReturn(Optional.of(applicationProcessHandle));
        when(applicationProcessHandle.destroy()).thenReturn(true);
        when(applicationProcessHandle.onExit()).thenReturn(CompletableFuture.completedFuture(applicationProcessHandle));

        command.run();
        final CommandStatus commandStatus = command.getCommandStatus();

        assertEquals(CommandStatus.SUCCESS, commandStatus);
    }

    @Test
    void testRunDestroyFailed() {
        when(processHandleProvider.findApplicationProcessHandle()).thenReturn(Optional.of(applicationProcessHandle));
        when(applicationProcessHandle.destroy()).thenReturn(true);
        when(applicationProcessHandle.onExit()).thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

        command.run();
        final CommandStatus commandStatus = command.getCommandStatus();

        assertEquals(CommandStatus.ERROR, commandStatus);
    }
}
