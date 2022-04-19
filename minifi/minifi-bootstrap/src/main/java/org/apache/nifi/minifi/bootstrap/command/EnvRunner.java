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
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;

import java.io.IOException;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;

public class EnvRunner implements CommandRunner {
    private static final String ENV_CMD = "ENV";

    private final MiNiFiCommandSender miNiFiCommandSender;
    private final CurrentPortProvider currentPortProvider;

    public EnvRunner(MiNiFiCommandSender miNiFiCommandSender, CurrentPortProvider currentPortProvider) {
        this.miNiFiCommandSender = miNiFiCommandSender;
        this.currentPortProvider = currentPortProvider;
    }

    /**
     * Returns information about the MiNiFi's virtual machine.
     * @param args the input arguments
     * @return status code
     */
    @Override
    public int runCommand(String[] args) {
        return env();
    }

    private int env() {
        Integer port = currentPortProvider.getCurrentPort();
        if (port == null) {
            CMD_LOGGER.error("Apache MiNiFi is not currently running");
            return MINIFI_NOT_RUNNING.getStatusCode();
        }

        try {
            miNiFiCommandSender.sendCommand(ENV_CMD, port).ifPresent(CMD_LOGGER::info);
        } catch (IOException e) {
            CMD_LOGGER.error("Failed to get ENV response from MiNiFi");
            DEFAULT_LOGGER.error("Exception:", e);
            return ERROR.getStatusCode();
        }

        return OK.getStatusCode();
    }
}
