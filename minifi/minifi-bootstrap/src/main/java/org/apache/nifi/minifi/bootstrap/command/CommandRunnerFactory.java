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

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.apache.nifi.minifi.bootstrap.BootstrapCommand;
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
import org.apache.nifi.minifi.bootstrap.util.ProcessUtils;

public class CommandRunnerFactory {

    private final MiNiFiCommandSender miNiFiCommandSender;
    private final CurrentPortProvider currentPortProvider;
    private final MiNiFiParameters miNiFiParameters;
    private final MiNiFiStatusProvider miNiFiStatusProvider;
    private final PeriodicStatusReporterManager periodicStatusReporterManager;
    private final BootstrapFileProvider bootstrapFileProvider;
    private final MiNiFiStdLogHandler miNiFiStdLogHandler;
    private final File bootstrapConfigFile;
    private final RunMiNiFi runMiNiFi;
    private final GracefulShutdownParameterProvider gracefulShutdownParameterProvider;
    private final MiNiFiExecCommandProvider miNiFiExecCommandProvider;
    private final ProcessUtils processUtils;
    private final ConfigurationChangeListener configurationChangeListener;

    public CommandRunnerFactory(MiNiFiCommandSender miNiFiCommandSender, CurrentPortProvider currentPortProvider, MiNiFiParameters miNiFiParameters,
        MiNiFiStatusProvider miNiFiStatusProvider, PeriodicStatusReporterManager periodicStatusReporterManager,
        BootstrapFileProvider bootstrapFileProvider, MiNiFiStdLogHandler miNiFiStdLogHandler, File bootstrapConfigFile, RunMiNiFi runMiNiFi,
        GracefulShutdownParameterProvider gracefulShutdownParameterProvider, MiNiFiExecCommandProvider miNiFiExecCommandProvider, ProcessUtils processUtils,
        ConfigurationChangeListener configurationChangeListener) {
        this.miNiFiCommandSender = miNiFiCommandSender;
        this.currentPortProvider = currentPortProvider;
        this.miNiFiParameters = miNiFiParameters;
        this.miNiFiStatusProvider = miNiFiStatusProvider;
        this.periodicStatusReporterManager = periodicStatusReporterManager;
        this.bootstrapFileProvider = bootstrapFileProvider;
        this.miNiFiStdLogHandler = miNiFiStdLogHandler;
        this.bootstrapConfigFile = bootstrapConfigFile;
        this.runMiNiFi = runMiNiFi;
        this.gracefulShutdownParameterProvider = gracefulShutdownParameterProvider;
        this.miNiFiExecCommandProvider = miNiFiExecCommandProvider;
        this.processUtils = processUtils;
        this.configurationChangeListener = configurationChangeListener;
    }

    /**
     * Returns a runner associated with the given command.
     * @param command the bootstrap command
     * @return the runner
     */
    public CommandRunner getRunner(BootstrapCommand command) {
        CommandRunner commandRunner = switch (command) {
            case START, RUN ->
                    new StartRunner(currentPortProvider, bootstrapFileProvider, periodicStatusReporterManager, miNiFiStdLogHandler, miNiFiParameters,
                            bootstrapConfigFile, runMiNiFi, miNiFiExecCommandProvider, configurationChangeListener);
            case STOP ->
                    new StopRunner(bootstrapFileProvider, miNiFiParameters, miNiFiCommandSender, currentPortProvider, gracefulShutdownParameterProvider, processUtils);
            case STATUS -> new StatusRunner(miNiFiParameters, miNiFiStatusProvider);
            case RESTART -> new CompositeCommandRunner(getRestartServices());
            case DUMP -> new DumpRunner(miNiFiCommandSender, currentPortProvider);
            case ENV -> new EnvRunner(miNiFiCommandSender, currentPortProvider);
            case FLOWSTATUS -> new FlowStatusRunner(periodicStatusReporterManager);
            default -> throw new IllegalArgumentException("Unknown MiNiFi bootstrap command");
        };
        return commandRunner;
    }

    private List<CommandRunner> getRestartServices() {
        List<CommandRunner> compositeList = new LinkedList<>();
        compositeList.add(new StopRunner(bootstrapFileProvider, miNiFiParameters, miNiFiCommandSender, currentPortProvider, gracefulShutdownParameterProvider, processUtils));
        compositeList.add(new StartRunner(currentPortProvider, bootstrapFileProvider, periodicStatusReporterManager, miNiFiStdLogHandler, miNiFiParameters,
            bootstrapConfigFile, runMiNiFi, miNiFiExecCommandProvider, configurationChangeListener));
        return compositeList;
    }
}
