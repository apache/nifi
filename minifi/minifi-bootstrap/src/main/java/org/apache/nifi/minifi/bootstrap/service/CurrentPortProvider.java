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

import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.util.ProcessUtils;

public class CurrentPortProvider {
    private final MiNiFiCommandSender miNiFiCommandSender;
    private final MiNiFiParameters miNiFiParameters;
    private final ProcessUtils processUtils;

    public CurrentPortProvider(MiNiFiCommandSender miNiFiCommandSender, MiNiFiParameters miNiFiParameters, ProcessUtils processUtils) {
        this.miNiFiCommandSender = miNiFiCommandSender;
        this.miNiFiParameters = miNiFiParameters;
        this.processUtils = processUtils;
    }

    public Integer getCurrentPort() {
        int miNiFiPort = miNiFiParameters.getMiNiFiPort();
        if (miNiFiPort == UNINITIALIZED) {
            DEFAULT_LOGGER.debug("Port is not defined");
            return null;
        }

        DEFAULT_LOGGER.debug("Current port: {}", miNiFiPort);

        boolean success = miNiFiCommandSender.isPingSuccessful(miNiFiPort);
        if (success) {
            DEFAULT_LOGGER.debug("Successful PING on port {}", miNiFiPort);
            return miNiFiPort;
        }

        long minifiPid = miNiFiParameters.getMinifiPid();
        DEFAULT_LOGGER.debug("Current PID {}", minifiPid);

        boolean procRunning = processUtils.isProcessRunning(minifiPid);
        if (procRunning) {
            return miNiFiPort;
        } else {
            return null;
        }
    }
}
