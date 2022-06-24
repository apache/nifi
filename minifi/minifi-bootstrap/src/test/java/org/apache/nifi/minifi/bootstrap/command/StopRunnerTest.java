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

package org.apache.nifi.minifi.bootstrap.command;

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.UNINITIALIZED;
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.apache.nifi.minifi.bootstrap.command.StopRunner.SHUTDOWN_CMD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.GracefulShutdownParameterProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;
import org.apache.nifi.minifi.bootstrap.util.ProcessUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StopRunnerTest {

    private static final int MINIFI_PORT = 1337;
    private static final long MINIFI_PID = 1;

    @Mock
    private BootstrapFileProvider bootstrapFileProvider;
    @Mock
    private MiNiFiParameters miNiFiParameters;
    @Mock
    private MiNiFiCommandSender miNiFiCommandSender;
    @Mock
    private CurrentPortProvider currentPortProvider;
    @Mock
    private GracefulShutdownParameterProvider gracefulShutdownParameterProvider;
    @Mock
    private ProcessUtils processUtils;

    @InjectMocks
    private StopRunner stopRunner;

    @Test
    void testRunCommandShouldReturnErrorStatusCodeInCaseOfException() {
        when(currentPortProvider.getCurrentPort()).thenThrow(new RuntimeException());

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(ERROR.getStatusCode(), statusCode);
        verifyNoInteractions(bootstrapFileProvider, miNiFiParameters, miNiFiCommandSender, gracefulShutdownParameterProvider, processUtils);
    }

    @Test
    void testRunCommandShouldReturnMiNiFiNotRunningStatusCodeInCaseMiNiFiPortIsNull() {
        when(currentPortProvider.getCurrentPort()).thenReturn(null);

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(MINIFI_NOT_RUNNING.getStatusCode(), statusCode);
        verifyNoInteractions(bootstrapFileProvider, miNiFiParameters, miNiFiCommandSender, gracefulShutdownParameterProvider, processUtils);
    }

    @Test
    void testRunCommandShouldCreateAndCleanupLockFileAfterExecution() throws IOException {
        File lockFile = mock(File.class);
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(bootstrapFileProvider.getLockFile()).thenReturn(lockFile);
        when(lockFile.exists()).thenReturn(false, true);
        when(lockFile.delete()).thenReturn(true);
        when(miNiFiParameters.getMinifiPid()).thenReturn((long) UNINITIALIZED);
        when(miNiFiCommandSender.sendCommand(SHUTDOWN_CMD, MINIFI_PORT)).thenReturn(Optional.of(SHUTDOWN_CMD));

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), statusCode);
        verify(lockFile).createNewFile();
        verifyNoInteractions(gracefulShutdownParameterProvider, processUtils);
    }

    @Test
    void testRunCommandShouldMessageBeLockedInCaseOfLockFileFailureIssue() throws IOException {
        File lockFile = mock(File.class);
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(bootstrapFileProvider.getLockFile()).thenReturn(lockFile);
        when(lockFile.exists()).thenReturn(false, true);
        when(lockFile.delete()).thenReturn(false);
        when(miNiFiParameters.getMinifiPid()).thenReturn((long) UNINITIALIZED);
        when(miNiFiCommandSender.sendCommand(SHUTDOWN_CMD, MINIFI_PORT)).thenReturn(Optional.of(SHUTDOWN_CMD));

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), statusCode);
        verify(lockFile).createNewFile();
        verifyNoInteractions(gracefulShutdownParameterProvider, processUtils);
    }

    @Test
    void testRunCommandShouldReturnErrorStatusCodeIfMiNiFiResponseIsNotShutdown() throws IOException {
        File lockFile = mock(File.class);
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(bootstrapFileProvider.getLockFile()).thenReturn(lockFile);
        when(lockFile.exists()).thenReturn(true, false);
        when(miNiFiParameters.getMinifiPid()).thenReturn((long) UNINITIALIZED);
        String unknown = "unknown";
        when(miNiFiCommandSender.sendCommand(SHUTDOWN_CMD, MINIFI_PORT)).thenReturn(Optional.of(unknown));

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(ERROR.getStatusCode(), statusCode);
        verifyNoInteractions(gracefulShutdownParameterProvider, processUtils);
    }

    @Test
    void testRunCommandShouldHandleExceptionalCaseIfProcessIdIsUnknown() throws IOException {
        File lockFile = mock(File.class);
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(bootstrapFileProvider.getLockFile()).thenReturn(lockFile);
        when(lockFile.exists()).thenReturn(true, false);
        when(miNiFiParameters.getMinifiPid()).thenReturn((long) UNINITIALIZED);
        when(miNiFiCommandSender.sendCommand(SHUTDOWN_CMD, MINIFI_PORT)).thenThrow(new IOException());

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), statusCode);
        verifyNoInteractions(gracefulShutdownParameterProvider, processUtils);
    }

    @Test
    void testRunCommandShouldHandleExceptionalCaseIfProcessIdIsGiven() throws IOException {
        File lockFile = mock(File.class);
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(bootstrapFileProvider.getLockFile()).thenReturn(lockFile);
        when(lockFile.exists()).thenReturn(true, false);
        when(miNiFiParameters.getMinifiPid()).thenReturn(MINIFI_PID);
        when(miNiFiCommandSender.sendCommand(SHUTDOWN_CMD, MINIFI_PORT)).thenThrow(new IOException());

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), statusCode);
        verify(processUtils).killProcessTree(MINIFI_PID);
        verifyNoInteractions(gracefulShutdownParameterProvider);
    }

    @Test
    void testRunCommandShouldShutDownMiNiFiProcessGracefully() throws IOException {
        File lockFile = mock(File.class);
        File statusFile = mock(File.class);
        File pidFile = mock(File.class);
        int gracefulShutdownSeconds = 10;
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(bootstrapFileProvider.getLockFile()).thenReturn(lockFile);
        when(bootstrapFileProvider.getStatusFile()).thenReturn(statusFile);
        when(bootstrapFileProvider.getPidFile()).thenReturn(pidFile);
        when(lockFile.exists()).thenReturn(true, false);
        when(statusFile.exists()).thenReturn(true);
        when(pidFile.exists()).thenReturn(true);
        when(statusFile.delete()).thenReturn(true);
        when(pidFile.delete()).thenReturn(true);
        when(miNiFiParameters.getMinifiPid()).thenReturn(MINIFI_PID);
        when(miNiFiCommandSender.sendCommand(SHUTDOWN_CMD, MINIFI_PORT)).thenReturn(Optional.of(SHUTDOWN_CMD));
        when(gracefulShutdownParameterProvider.getGracefulShutdownSeconds()).thenReturn(gracefulShutdownSeconds);

        int statusCode = stopRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), statusCode);
        verify(statusFile).delete();
        verify(pidFile).delete();
        verify(processUtils).shutdownProcess(eq(MINIFI_PID), anyString(), eq(gracefulShutdownSeconds));
    }
}