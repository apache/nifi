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

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.DEFAULT_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.UNINITIALIZED;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.util.UnixProcessUtils;

public class ReloadService {
    private final BootstrapFileProvider bootstrapFileProvider;
    private final MiNiFiParameters miNiFiParameters;
    private final MiNiFiCommandSender miNiFiCommandSender;
    private static final String RELOAD_CMD = "RELOAD";
    private final CurrentPortProvider currentPortProvider;
    private final GracefulShutdownParameterProvider gracefulShutdownParameterProvider;
    private final RunMiNiFi runMiNiFi;

    public ReloadService(BootstrapFileProvider bootstrapFileProvider, MiNiFiParameters miNiFiParameters,
        MiNiFiCommandSender miNiFiCommandSender, CurrentPortProvider currentPortProvider,
        GracefulShutdownParameterProvider gracefulShutdownParameterProvider, RunMiNiFi runMiNiFi) {
        this.bootstrapFileProvider = bootstrapFileProvider;
        this.miNiFiParameters = miNiFiParameters;
        this.miNiFiCommandSender = miNiFiCommandSender;
        this.currentPortProvider = currentPortProvider;
        this.gracefulShutdownParameterProvider = gracefulShutdownParameterProvider;
        this.runMiNiFi = runMiNiFi;
    }

    public void reload() throws IOException {
        // indicate that a reload command is in progress
        File reloadLockFile = bootstrapFileProvider.getReloadLockFile();
        if (!reloadLockFile.exists()) {
            reloadLockFile.createNewFile();
        }

        long minifiPid = miNiFiParameters.getMinifiPid();
        try {
            Optional<String> commandResponse = miNiFiCommandSender.sendCommand(RELOAD_CMD, currentPortProvider.getCurrentPort());
            if (commandResponse.filter(RELOAD_CMD::equals).isPresent()) {
                DEFAULT_LOGGER.info("Apache MiNiFi has accepted the Reload Command and is reloading");
                if (minifiPid != UNINITIALIZED) {
                    UnixProcessUtils.gracefulShutDownMiNiFiProcess(minifiPid, "MiNiFi has not finished shutting down after {} seconds as part of configuration reload. Killing process.",
                        gracefulShutdownParameterProvider.getGracefulShutdownSeconds());
                    runMiNiFi.setReloading(true);
                    DEFAULT_LOGGER.info("MiNiFi has finished shutting down and will be reloaded.");
                }
            } else {
                DEFAULT_LOGGER.error("When sending RELOAD command to MiNiFi, got unexpected response {}.", commandResponse.orElse(null));
            }
        } catch (IOException e) {
            if (minifiPid == UNINITIALIZED) {
                DEFAULT_LOGGER.error("No PID found for the MiNiFi process, so unable to kill process; The process should be killed manually.");
            } else {
                DEFAULT_LOGGER.error("Will kill the MiNiFi Process with PID {}", minifiPid);
                UnixProcessUtils.killProcessTree(minifiPid);
            }
        } finally {
            if (reloadLockFile.exists() && !reloadLockFile.delete()) {
                DEFAULT_LOGGER.error("Failed to delete reload lock file {}; this file should be cleaned up manually", reloadLockFile);
            }
        }
    }
}
