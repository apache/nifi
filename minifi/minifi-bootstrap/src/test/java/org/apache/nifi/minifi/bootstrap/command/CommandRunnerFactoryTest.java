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

import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.DUMP;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.ENV;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.FLOWSTATUS;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.RESTART;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.RUN;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.START;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.STATUS;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.STOP;
import static org.apache.nifi.minifi.bootstrap.BootstrapCommand.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.GracefulShutdownParameterProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStatusProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler;
import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CommandRunnerFactoryTest {

    @Mock
    private MiNiFiCommandSender miNiFiCommandSender;
    @Mock
    private CurrentPortProvider currentPortProvider;
    @Mock
    private MiNiFiParameters miNiFiParameters;
    @Mock
    private MiNiFiStatusProvider miNiFiStatusProvider;
    @Mock
    private PeriodicStatusReporterManager periodicStatusReporterManager;
    @Mock
    private BootstrapFileProvider bootstrapFileProvider;
    @Mock
    private MiNiFiStdLogHandler miNiFiStdLogHandler;
    @Mock
    private File bootstrapConfigFile;
    @Mock
    private RunMiNiFi runMiNiFi;
    @Mock
    private GracefulShutdownParameterProvider gracefulShutdownParameterProvider;
    @Mock
    private MiNiFiExecCommandProvider miNiFiExecCommandProvider;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private ConfigurationChangeListener configurationChangeListener;

    @InjectMocks
    private CommandRunnerFactory commandRunnerFactory;

    @Test
    void testRunCommandShouldStartCommandReturnStartRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(START);

        assertInstanceOf(StartRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldRunCommandReturnStartRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(RUN);

        assertInstanceOf(StartRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldStopCommandReturnStopRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(STOP);

        assertInstanceOf(StopRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldEnvCommandReturnEnvRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(ENV);

        assertInstanceOf(EnvRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldDumpCommandReturnDumpRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(DUMP);

        assertInstanceOf(DumpRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldFlowStatusCommandReturnFlowStatusRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(FLOWSTATUS);

        assertInstanceOf(FlowStatusRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldStatusCommandReturnStatusRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(STATUS);

        assertInstanceOf(StatusRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldRestartCommandReturnCompositeRunner() {
        CommandRunner runner = commandRunnerFactory.getRunner(RESTART);

        assertInstanceOf(CompositeCommandRunner.class, runner);
        verify();
    }

    @Test
    void testRunCommandShouldThrowIllegalArgumentExceptionInCaseOfUnknownCommand() {
        assertThrows(IllegalArgumentException.class, () -> commandRunnerFactory.getRunner(UNKNOWN));
    }

    private void verify() {
        verifyNoInteractions(miNiFiCommandSender, currentPortProvider, miNiFiParameters, miNiFiStatusProvider, periodicStatusReporterManager, bootstrapFileProvider,
            miNiFiStdLogHandler, bootstrapConfigFile, runMiNiFi, gracefulShutdownParameterProvider, miNiFiExecCommandProvider, objectMapper, configurationChangeListener);
    }
}