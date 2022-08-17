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

import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.apache.nifi.minifi.bootstrap.command.EnvRunner.ENV_CMD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EnvRunnerTest {

    private static final int MINIFI_PORT = 1337;
    private static final String ENV_DATA = "ENV_DATA";

    @Mock
    private MiNiFiCommandSender miNiFiCommandSender;
    @Mock
    private CurrentPortProvider currentPortProvider;

    @InjectMocks
    private EnvRunner envRunner;

    @Test
    void testRunCommandShouldReturnNotRunningStatusCodeIfPortReturnsNull() {
        when(currentPortProvider.getCurrentPort()).thenReturn(null);

        int statusCode = envRunner.runCommand(new String[]{});

        assertEquals(MINIFI_NOT_RUNNING.getStatusCode(), statusCode);
        verifyNoMoreInteractions(currentPortProvider);
        verifyNoInteractions(miNiFiCommandSender);
    }

    @Test
    void testRunCommandShouldReturnErrorStatusCodeIfSendCommandThrowsException() throws IOException {
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(miNiFiCommandSender.sendCommand(ENV_CMD, MINIFI_PORT)).thenThrow(new IOException());

        int statusCode = envRunner.runCommand(new String[]{});

        assertEquals(ERROR.getStatusCode(), statusCode);
        verifyNoMoreInteractions(currentPortProvider, miNiFiCommandSender);
    }

    @Test
    void testRunCommandShouldReturnOkStatusCode() throws IOException {
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(miNiFiCommandSender.sendCommand(ENV_CMD, MINIFI_PORT)).thenReturn(Optional.of(ENV_DATA));

        int statusCode = envRunner.runCommand(new String[]{});

        assertEquals(OK.getStatusCode(), statusCode);
        verifyNoMoreInteractions(currentPortProvider, miNiFiCommandSender);
    }
}