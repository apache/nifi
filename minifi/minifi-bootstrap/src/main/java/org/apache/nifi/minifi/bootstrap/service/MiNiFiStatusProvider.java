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
import static org.apache.nifi.minifi.bootstrap.util.UnixProcessUtils.isProcessRunning;

import org.apache.nifi.minifi.bootstrap.MiNiFiStatus;

public class MiNiFiStatusProvider {

    private final MiNiFiCommandSender miNiFiCommandSender;

    public MiNiFiStatusProvider(MiNiFiCommandSender miNiFiCommandSender) {
        this.miNiFiCommandSender = miNiFiCommandSender;
    }

    public MiNiFiStatus getStatus(int port, long pid) {
        if (port == UNINITIALIZED && pid == UNINITIALIZED) {
            return new MiNiFiStatus();
        }

        boolean pingSuccess = false;
        if (port != UNINITIALIZED) {
            pingSuccess = miNiFiCommandSender.isPingSuccessful(port);
        }

        if (pingSuccess) {
            return new MiNiFiStatus(port, pid, true, true);
        }

        return new MiNiFiStatus(port, pid, false, isProcessRunning(pid));
    }
}
