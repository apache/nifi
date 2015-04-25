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
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.remote.protocol.CommunicationsSession;

public class Peer implements Communicant {

    private final PeerDescription description;
    private final CommunicationsSession commsSession;
    private final String url;
    private final String clusterUrl;
    private final String host;
    private final int port;

    private final Map<String, Long> penaltyExpirationMap = new HashMap<>();
    private boolean closed = false;

    public Peer(final PeerDescription description, final CommunicationsSession commsSession, final String peerUrl, final String clusterUrl) {
        this.description = description;
        this.commsSession = commsSession;
        this.url = peerUrl;
        this.clusterUrl = clusterUrl;

        try {
            final URI uri = new URI(peerUrl);
            this.port = uri.getPort();
            this.host = uri.getHost();
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid URL: " + peerUrl);
        }
    }

    public PeerDescription getDescription() {
        return description;
    }

    @Override
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
            commsSession.getInput().consume();
        } finally {
            commsSession.close();
        }
    }

    /**
     * Penalizes this peer for the given destination only for the provided
     * number of milliseconds
     *
     * @param destinationId id of destination
     * @param millis period of time to penalize peer
     */
    public void penalize(final String destinationId, final long millis) {
        final Long currentPenalty = penaltyExpirationMap.get(destinationId);
        final long proposedPenalty = System.currentTimeMillis() + millis;
        if (currentPenalty == null || proposedPenalty > currentPenalty) {
            penaltyExpirationMap.put(destinationId, proposedPenalty);
        }
    }

    public boolean isPenalized(final String destinationId) {
        final Long currentPenalty = penaltyExpirationMap.get(destinationId);
        return (currentPenalty != null && currentPenalty > System.currentTimeMillis());
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
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
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getDistinguishedName() {
        return commsSession.getUserDn();
    }
}
