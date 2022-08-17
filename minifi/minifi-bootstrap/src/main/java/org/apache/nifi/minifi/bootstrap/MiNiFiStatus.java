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

public class MiNiFiStatus {

    private final Integer port;
    private final Long pid;
    private final boolean respondingToPing;
    private final boolean processRunning;

    public MiNiFiStatus() {
        this.port = null;
        this.pid = null;
        this.respondingToPing = false;
        this.processRunning = false;
    }

    public MiNiFiStatus(Integer port, Long pid, boolean respondingToPing, boolean processRunning) {
        this.port = port;
        this.pid = pid;
        this.respondingToPing = respondingToPing;
        this.processRunning = processRunning;
    }

    public Long getPid() {
        return pid;
    }

    public Integer getPort() {
        return port;
    }

    public boolean isRespondingToPing() {
        return respondingToPing;
    }

    public boolean isProcessRunning() {
        return processRunning;
    }
}
