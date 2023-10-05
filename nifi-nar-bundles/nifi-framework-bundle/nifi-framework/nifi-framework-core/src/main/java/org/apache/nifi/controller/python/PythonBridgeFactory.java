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
package org.apache.nifi.controller.python;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.DisabledPythonBridge;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBridgeInitializationContext;
import org.apache.nifi.python.PythonProcessConfig;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonBridgeFactory {

    public static final String STANDARD_PYTHON_BRIDGE_IMPLEMENTATION_CLASS = "org.apache.nifi.py4j.StandardPythonBridge";
    private static final Logger LOG = LoggerFactory.getLogger(PythonBridgeFactory.class);

    public static PythonBridge createPythonBridge(final NiFiProperties nifiProperties, final ControllerServiceProvider serviceProvider, ExtensionManager extensionManager) {
        if (nifiProperties.isPythonProcessorsDisabled()) {
            LOG.info("Python Extensions disabled because the nifi.python.enabled property has been configured to false in nifi.properties");
            return new DisabledPythonBridge();
        }

        final String pythonCommand = nifiProperties.getProperty(NiFiProperties.PYTHON_COMMAND);
        if (StringUtils.isEmpty(pythonCommand)) {
            throw new RuntimeException("nifi.python.command is not configured properly!");
        }

        final String commsTimeout = nifiProperties.getProperty(NiFiProperties.PYTHON_COMMS_TIMEOUT);
        final File pythonFrameworkSourceDirectory = nifiProperties.getPythonFrameworkSourceDirectory();
        final List<File> pythonExtensionsDirectories = nifiProperties.getPythonExtensionsDirectories();
        final File pythonWorkingDirectory = new File(nifiProperties.getProperty(NiFiProperties.PYTHON_WORKING_DIRECTORY));
        final File pythonLogsDirectory = new File(nifiProperties.getProperty(NiFiProperties.PYTHON_LOGS_DIRECTORY));

        int maxProcesses = nifiProperties.getIntegerProperty(NiFiProperties.PYTHON_MAX_PROCESSES, 20);
        int maxProcessesPerType = nifiProperties.getIntegerProperty(NiFiProperties.PYTHON_MAX_PROCESSES_PER_TYPE, 2);

        final boolean enableControllerDebug = Boolean.parseBoolean(nifiProperties.getProperty(NiFiProperties.PYTHON_CONTROLLER_DEBUGPY_ENABLED, "false"));
        final int debugPort = nifiProperties.getIntegerProperty(NiFiProperties.PYTHON_CONTROLLER_DEBUGPY_PORT, 5678);
        final String debugHost = nifiProperties.getProperty(NiFiProperties.PYTHON_CONTROLLER_DEBUGPY_HOST, "localhost");
        final String debugLogs = nifiProperties.getProperty(NiFiProperties.PYTHON_CONTROLLER_DEBUGPY_LOGS_DIR, "logs");

        // Validate configuration for max numbers of processes.
        if (maxProcessesPerType < 1) {
            LOG.warn("Configured value for {} in nifi.properties is {}, which is invalid. Defaulting to 2.", NiFiProperties.PYTHON_MAX_PROCESSES_PER_TYPE, maxProcessesPerType);
            maxProcessesPerType = 2;
        }
        if (maxProcesses < 0) {
            LOG.warn("Configured value for {} in nifi.properties is {}, which is invalid. Defaulting to 20.", NiFiProperties.PYTHON_MAX_PROCESSES, maxProcessesPerType);
            maxProcesses = 20;
        }
        if (maxProcesses == 0) {
            LOG.warn("Will not enable Python Extensions because the {} property in nifi.properties is set to 0.", NiFiProperties.PYTHON_MAX_PROCESSES);
            return new DisabledPythonBridge();
        }
        if (maxProcessesPerType > maxProcesses) {
            LOG.warn("Configured values for {} and {} in nifi.properties are {} and {} (respectively), which is invalid. " +
                            "Cannot set max process count per extension type greater than the max number of processors. Setting both to {}",
                    NiFiProperties.PYTHON_MAX_PROCESSES_PER_TYPE, NiFiProperties.PYTHON_MAX_PROCESSES, maxProcessesPerType, maxProcesses, maxProcesses);

            maxProcessesPerType = maxProcesses;
        }

        final PythonProcessConfig pythonProcessConfig = new PythonProcessConfig.Builder()
                .pythonCommand(pythonCommand)
                .pythonFrameworkDirectory(pythonFrameworkSourceDirectory)
                .pythonExtensionsDirectories(pythonExtensionsDirectories)
                .pythonLogsDirectory(pythonLogsDirectory)
                .pythonWorkingDirectory(pythonWorkingDirectory)
                .commsTimeout(commsTimeout == null ? null : Duration.ofMillis(FormatUtils.getTimeDuration(commsTimeout, TimeUnit.MILLISECONDS)))
                .maxPythonProcesses(maxProcesses)
                .maxPythonProcessesPerType(maxProcessesPerType)
                .enableControllerDebug(enableControllerDebug)
                .debugPort(debugPort)
                .debugHost(debugHost)
                .debugLogsDirectory(new File(debugLogs))
                .build();

        final ControllerServiceTypeLookup serviceTypeLookup = serviceProvider::getControllerServiceType;

        try {
            final PythonBridge bridge = NarThreadContextClassLoader.createInstance(extensionManager, STANDARD_PYTHON_BRIDGE_IMPLEMENTATION_CLASS, PythonBridge.class, null);

            final PythonBridgeInitializationContext initializationContext = new PythonBridgeInitializationContext() {
                @Override
                public PythonProcessConfig getPythonProcessConfig() {
                    return pythonProcessConfig;
                }

                @Override
                public ControllerServiceTypeLookup getControllerServiceTypeLookup() {
                    return serviceTypeLookup;
                }
            };

            bridge.initialize(initializationContext);
            return bridge;
        } catch (final Exception e) {
            throw new RuntimeException("Python Bridge initialization failed", e);
        }
    }

}
