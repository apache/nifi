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

package org.apache.nifi.minifi.bootstrap.service;

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.UNINITIALIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.util.ProcessUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CurrentPortProviderTest {

    private static final int PORT = 1;
    private static final long PID = 1L;

    @Mock
    private MiNiFiCommandSender miNiFiCommandSender;
    @Mock
    private MiNiFiParameters miNiFiParameters;
    @Mock
    private ProcessUtils processUtils;

    @InjectMocks
    private CurrentPortProvider currentPortProvider;

    @Test
    void testGetCurrentPortShouldReturnNullIfMiNiFiPortIsNotSet() {
        when(miNiFiParameters.getMiNiFiPort()).thenReturn(UNINITIALIZED);

        assertNull(currentPortProvider.getCurrentPort());
        verifyNoInteractions(miNiFiCommandSender, processUtils);
    }

    @Test
    void testGetCurrentPortShouldReturnNullIfPingIsNotSuccessfulAndProcessIsNotRunning() {
        when(miNiFiParameters.getMiNiFiPort()).thenReturn(PORT);
        when(miNiFiCommandSender.isPingSuccessful(PORT)).thenReturn(false);
        when(miNiFiParameters.getMinifiPid()).thenReturn(PID);
        when(processUtils.isProcessRunning(PID)).thenReturn(false);

        assertNull(currentPortProvider.getCurrentPort());
    }

    @Test
    void testGetCurrentPortShouldReturnPortIfPingIsSuccessful() {
        when(miNiFiParameters.getMiNiFiPort()).thenReturn(PORT);
        when(miNiFiCommandSender.isPingSuccessful(PORT)).thenReturn(true);

        Integer currentPort = currentPortProvider.getCurrentPort();

        assertEquals(PORT, currentPort);
        verifyNoInteractions(processUtils);
        verifyNoMoreInteractions(miNiFiParameters);
    }

    @Test
    void testGetCurrentPortShouldReturnPortIfPingIsNotSuccessfulButPidIsRunning() {
        when(miNiFiParameters.getMiNiFiPort()).thenReturn(PORT);
        when(miNiFiCommandSender.isPingSuccessful(PORT)).thenReturn(false);
        when(miNiFiParameters.getMinifiPid()).thenReturn(PID);
        when(processUtils.isProcessRunning(PID)).thenReturn(true);

        Integer currentPort = currentPortProvider.getCurrentPort();

        assertEquals(PORT, currentPort);
    }
}