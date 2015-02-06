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

import java.io.IOException;
import java.net.URI;

import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

public class Peer {

    private final CommunicationsSession commsSession;
    private final String url;
    private final String clusterUrl;
    private final String host;
    private long penalizationExpiration = 0L;
    private boolean closed = false;

    public Peer(final CommunicationsSession commsSession, final String peerUrl, final String clusterUrl) {
        this.commsSession = commsSession;
        this.url = peerUrl;
        this.clusterUrl = clusterUrl;

        try {
            this.host = new URI(peerUrl).getHost();
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid URL: " + peerUrl);
        }
    }

    public String getUrl() {
        return url;
    }
    
    public String getClusterUrl() {
    	return clusterUrl;
    }

    public CommunicationsSession getCommunicationsSession() {
        return commsSession;
    }

    public void close() throws IOException {
        this.closed = true;

        // Consume the InputStream so that it doesn't linger on the Peer's outgoing socket buffer
        try {
            StreamUtils.copy(commsSession.getInput().getInputStream(), new NullOutputStream());
        } finally {
            commsSession.close();
        }
    }

    public void penalize(final long millis) {
        penalizationExpiration = Math.max(penalizationExpiration, System.currentTimeMillis() + millis);
    }

    public boolean isPenalized() {
        return penalizationExpiration > System.currentTimeMillis();
    }

    public boolean isClosed() {
        return closed;
    }

    public String getHost() {
        return host;
    }

    @Override
    public int hashCode() {
        return 8320 + url.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Peer)) {
            return false;
        }

        final Peer other = (Peer) obj;
        return this.url.equals(other.url);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Peer[url=").append(url);
        if (closed) {
            sb.append(",CLOSED");
        } else if (isPenalized()) {
            sb.append(",PENALIZED");
        }
        sb.append("]");
        return sb.toString();
    }
}
