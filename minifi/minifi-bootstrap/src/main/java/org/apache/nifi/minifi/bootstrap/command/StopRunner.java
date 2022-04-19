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

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CMD_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.DEFAULT_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.UNINITIALIZED;
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.GracefulShutdownParameterProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;
import org.apache.nifi.minifi.bootstrap.util.UnixProcessUtils;

public class StopRunner implements CommandRunner {
    private static final String SHUTDOWN_CMD = "SHUTDOWN";

    private final BootstrapFileProvider bootstrapFileProvider;
    private final MiNiFiParameters miNiFiParameters;
    private final MiNiFiCommandSender miNiFiCommandSender;
    private final CurrentPortProvider currentPortProvider;
    private final GracefulShutdownParameterProvider gracefulShutdownParameterProvider;

    public StopRunner(BootstrapFileProvider bootstrapFileProvider, MiNiFiParameters miNiFiParameters, MiNiFiCommandSender miNiFiCommandSender,
        CurrentPortProvider currentPortProvider, GracefulShutdownParameterProvider gracefulShutdownParameterProvider) {
        this.bootstrapFileProvider = bootstrapFileProvider;
        this.miNiFiParameters = miNiFiParameters;
        this.miNiFiCommandSender = miNiFiCommandSender;
        this.currentPortProvider = currentPortProvider;
        this.gracefulShutdownParameterProvider = gracefulShutdownParameterProvider;
    }

    /**
     * Shutdown the MiNiFi and the managing bootstrap process as well.
     * @param args the input arguments
     * @return status code
     */
    @Override
    public int runCommand(String[] args) {
        try {
            return stop();
        } catch (Exception e) {
            DEFAULT_LOGGER.error("Exception happened during stopping MiNiFi", e);
            return ERROR.getStatusCode();
        }
    }

    private int stop() throws IOException {
        Integer currentPort = currentPortProvider.getCurrentPort();
        if (currentPort == null) {
            CMD_LOGGER.error("Apache MiNiFi is not currently running");
            return MINIFI_NOT_RUNNING.getStatusCode();
        }

        int status = OK.getStatusCode();
        // indicate that a stop command is in progress
        File lockFile = bootstrapFileProvider.getLockFile();
        if (!lockFile.exists()) {
            lockFile.createNewFile();
        }

        File statusFile = bootstrapFileProvider.getStatusFile();
        File pidFile = bootstrapFileProvider.getPidFile();
        long minifiPid = miNiFiParameters.getMinifiPid();

        try {
            Optional<String> commandResponse = miNiFiCommandSender.sendCommand(SHUTDOWN_CMD, currentPort);
            if (commandResponse.filter(SHUTDOWN_CMD::equals).isPresent()) {
                CMD_LOGGER.info("Apache MiNiFi has accepted the Shutdown Command and is shutting down now");

                if (minifiPid != UNINITIALIZED) {
                    UnixProcessUtils.gracefulShutDownMiNiFiProcess(minifiPid, "MiNiFi has not finished shutting down after {} seconds. Killing process.",
                        gracefulShutdownParameterProvider.getGracefulShutdownSeconds());

                    if (statusFile.exists() && !statusFile.delete()) {
                        CMD_LOGGER.error("Failed to delete status file {}; this file should be cleaned up manually", statusFile);
                    }

                    if (pidFile.exists() && !pidFile.delete()) {
                        CMD_LOGGER.error("Failed to delete pid file {}; this file should be cleaned up manually", pidFile);
                    }

                    CMD_LOGGER.info("MiNiFi has finished shutting down.");
                }
            } else {
                CMD_LOGGER.error("When sending SHUTDOWN command to MiNiFi, got unexpected response {}", commandResponse.orElse(null));
                status = ERROR.getStatusCode();
            }
        } catch (IOException e) {
            if (minifiPid == UNINITIALIZED) {
                DEFAULT_LOGGER.error("No PID found for the MiNiFi process, so unable to kill process; The process should be killed manually.");
            } else {
                DEFAULT_LOGGER.error("Will kill the MiNiFi Process with PID {}", minifiPid);
                UnixProcessUtils.killProcessTree(minifiPid);
            }
        } finally {
            if (lockFile.exists() && !lockFile.delete()) {
                CMD_LOGGER.error("Failed to delete lock file {}; this file should be cleaned up manually", lockFile);
            }
        }

        return status;
    }
}
