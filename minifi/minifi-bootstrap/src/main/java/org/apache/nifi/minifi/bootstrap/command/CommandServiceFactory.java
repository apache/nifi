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
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.RunMiNiFiCommand;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.GracefulShutdownParameterProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStatusProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler;
import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;

public class CommandServiceFactory {

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

    public CommandServiceFactory(MiNiFiCommandSender miNiFiCommandSender, CurrentPortProvider currentPortProvider, MiNiFiParameters miNiFiParameters,
        MiNiFiStatusProvider miNiFiStatusProvider, PeriodicStatusReporterManager periodicStatusReporterManager,
        BootstrapFileProvider bootstrapFileProvider, MiNiFiStdLogHandler miNiFiStdLogHandler, File bootstrapConfigFile, RunMiNiFi runMiNiFi,
        GracefulShutdownParameterProvider gracefulShutdownParameterProvider) {
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
    }

    public CommandService getService(RunMiNiFiCommand command) {
        CommandService commandService;
        switch (command) {
            case START:
            case RUN:
                commandService = new StartService(currentPortProvider, bootstrapFileProvider, periodicStatusReporterManager, miNiFiStdLogHandler, miNiFiParameters,
                    bootstrapConfigFile, runMiNiFi);
                break;
            case STOP:
                commandService = new StopService(bootstrapFileProvider, miNiFiParameters, miNiFiCommandSender, currentPortProvider, gracefulShutdownParameterProvider);
                break;
            case STATUS:
                commandService = new StatusService(miNiFiParameters, miNiFiStatusProvider);
                break;
            case RESTART:
                commandService = new CompositeService(getRestartServices());
                break;
            case DUMP:
                commandService = new DumpService(miNiFiCommandSender, currentPortProvider);
                break;
            case ENV:
                commandService = new EnvService(miNiFiParameters, miNiFiStatusProvider);
                break;
            case FLOWSTATUS:
                commandService = new FlowStatusService(periodicStatusReporterManager);
                break;
            default:
                throw new IllegalArgumentException("Unknown MiNiFi bootstrap command");
        }
        return commandService;
    }

    private List<CommandService> getRestartServices() {
        List<CommandService> compositeList = new LinkedList<>();
        compositeList.add(new StopService(bootstrapFileProvider, miNiFiParameters, miNiFiCommandSender, currentPortProvider, gracefulShutdownParameterProvider));
        compositeList.add(new StartService(currentPortProvider, bootstrapFileProvider, periodicStatusReporterManager, miNiFiStdLogHandler, miNiFiParameters,
            bootstrapConfigFile, runMiNiFi));
        return compositeList;
    }
}
