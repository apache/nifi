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

import org.apache.nifi.web.api.dto.remote.PeerDTO;

public class PeerDescription {

    private final String hostname;
    private final int port;
    private final boolean secure;

    public PeerDescription(final String hostname, final int port, final boolean secure) {
        this.hostname = hostname;
        this.port = port;
        this.secure = secure;
    }

    public PeerDescription(final PeerDTO peerDTO) {
        this.hostname = peerDTO.getHostname();
        this.port = peerDTO.getPort();
        this.secure = peerDTO.isSecure();
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

    @Override
    public String toString() {
        return "PeerDescription[hostname=" + hostname + ", port=" + port + ", secure=" + secure + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final PeerDescription other = (PeerDescription) obj;
        if (hostname == null) {
            if (other.hostname != null) {
                return false;
            }
        } else if (!hostname.equals(other.hostname)) {
            return false;
        }

        return port == other.port;
    }
}
