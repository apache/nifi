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
package org.apache.nifi.remote;

public class PeerStatus {

    private final String hostname;
    private final int port;
    private final boolean secure;
    private final int numFlowFiles;

    public PeerStatus(final String hostname, final int port, final boolean secure, final int numFlowFiles) {
        this.hostname = hostname;
        this.port = port;
        this.secure = secure;
        this.numFlowFiles = numFlowFiles;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public boolean isSecure() {
        return secure;
    }

    public int getFlowFileCount() {
        return numFlowFiles;
    }

    @Override
    public String toString() {
        return "PeerStatus[hostname=" + hostname + ",port=" + port + ",secure=" + secure + ",flowFileCount=" + numFlowFiles + "]";
    }

    @Override
    public int hashCode() {
        return 9824372 + hostname.hashCode() + port;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof PeerStatus)) {
            return false;
        }

        final PeerStatus other = (PeerStatus) obj;
        return port == other.port && hostname.equals(other.hostname);
    }
}
