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

package org.apache.nifi.minifi.bootstrap;

import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.STOP;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler;
import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ShutdownHookTest {

    @Mock
    private RunMiNiFi runner;
    @Mock
    private MiNiFiStdLogHandler miNiFiStdLogHandler;
    @Mock
    private PeriodicStatusReporterManager periodicStatusReporterManager;

    @InjectMocks
    private ShutdownHook shutdownHook;

    @Test
    @SuppressWarnings("PMD.DontCallThreadRun")
    void testRunShouldShutdownSchedulersAndProcesses() {
        when(runner.getPeriodicStatusReporterManager()).thenReturn(periodicStatusReporterManager);

        shutdownHook.run();

        verify(miNiFiStdLogHandler).shutdown();
        verify(runner).shutdownChangeNotifier();
        verify(runner).getPeriodicStatusReporterManager();
        verify(periodicStatusReporterManager).shutdownPeriodicStatusReporters();
        verify(runner).setAutoRestartNiFi(false);
        verify(runner).run(STOP);
    }
}