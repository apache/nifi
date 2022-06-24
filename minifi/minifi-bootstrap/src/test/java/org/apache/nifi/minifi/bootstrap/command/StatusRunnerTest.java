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

import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RESPONDING;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.MiNiFiStatus;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStatusProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StatusRunnerTest {

    private static final long MINIFI_PID = 1L;
    private static final int MINIFI_PORT = 1337;

    @Mock
    private MiNiFiParameters miNiFiParameters;
    @Mock
    private MiNiFiStatusProvider miNiFiStatusProvider;

    @InjectMocks
    private StatusRunner statusRunner;

    @BeforeEach
    void setup() {
        when(miNiFiParameters.getMinifiPid()).thenReturn(MINIFI_PID);
        when(miNiFiParameters.getMiNiFiPort()).thenReturn(MINIFI_PORT);
    }

    @Test
    void testRunCommandShouldReturnOkStatusIfMiNiFiIsRespondingToPing() {
        when(miNiFiStatusProvider.getStatus(MINIFI_PORT, MINIFI_PID)).thenReturn(new MiNiFiStatus(MINIFI_PORT, MINIFI_PID, true, false));

        int status = statusRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), status);
    }

    @Test
    void testRunCommandShouldReturnMiNiFiNotRespondingStatusIfMiNiFiIsNotRespondingToPing() {
        when(miNiFiStatusProvider.getStatus(MINIFI_PORT, MINIFI_PID)).thenReturn(new MiNiFiStatus(MINIFI_PORT, MINIFI_PID, false, true));

        int status = statusRunner.runCommand(new String[0]);

        assertEquals(MINIFI_NOT_RESPONDING.getStatusCode(), status);
    }

    @Test
    void testRunCommandShouldReturnMiNiFiNotRunningStatusIfMiNiFiIsNotRespondingToPingAndProcessIsNotRunning() {
        when(miNiFiStatusProvider.getStatus(MINIFI_PORT, MINIFI_PID)).thenReturn(new MiNiFiStatus(null, MINIFI_PID, false, false));

        int status = statusRunner.runCommand(new String[0]);

        assertEquals(MINIFI_NOT_RUNNING.getStatusCode(), status);
    }

    @Test
    void testRunCommandShouldReturnMiNiFiNotRunningStatusIfMiNiFiIsNotRespondingToPingIfPortIsGivenButPidIsMissingAndNotRespondingToPing() {
        when(miNiFiStatusProvider.getStatus(MINIFI_PORT, MINIFI_PID)).thenReturn(new MiNiFiStatus(MINIFI_PORT, null, false, false));

        int status = statusRunner.runCommand(new String[0]);

        assertEquals(MINIFI_NOT_RUNNING.getStatusCode(), status);
    }

    @Test
    void testRunCommandShouldReturnMiNiFiNotRunningStatusIfPortAndPidIsGivenButNotRespondingToPingAndProcessIsNotRunning() {
        when(miNiFiStatusProvider.getStatus(MINIFI_PORT, MINIFI_PID)).thenReturn(new MiNiFiStatus(MINIFI_PORT, MINIFI_PID, false, false));

        int status = statusRunner.runCommand(new String[0]);

        assertEquals(MINIFI_NOT_RUNNING.getStatusCode(), status);
    }
}