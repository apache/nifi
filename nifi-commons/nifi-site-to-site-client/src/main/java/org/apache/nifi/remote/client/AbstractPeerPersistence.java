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
package org.apache.nifi.remote.client;

import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.util.PeerStatusCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class AbstractPeerPersistence implements PeerPersistence {

    static final String REMOTE_INSTANCE_URIS_PREFIX = "Remote Instance URIs: ";

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected PeerStatusCache restorePeerStatuses(final BufferedReader reader,
                                     long cachedTimestamp) throws IOException {
        final SiteToSiteTransportProtocol transportProtocol;
        try {
            transportProtocol = SiteToSiteTransportProtocol.valueOf(reader.readLine());
        } catch (IllegalArgumentException e) {
            logger.info("Discard stored peer statuses in {} because transport protocol is not stored",
                this.getClass().getSimpleName());
            return null;
        }

        String line = reader.readLine();
        if (line == null || !line.startsWith(REMOTE_INSTANCE_URIS_PREFIX)) {
            logger.info("Discard stored peer statuses in {} because remote instance URIs are not stored",
                    this.getClass().getSimpleName());
            return null;
        }
        final String remoteInstanceUris = line.substring(REMOTE_INSTANCE_URIS_PREFIX.length());

        final Set<PeerStatus> restoredStatuses = readPeerStatuses(reader);

        if (!restoredStatuses.isEmpty()) {
            logger.info("Restored peer statuses from {} {}", this.getClass().getSimpleName(), restoredStatuses);
            return new PeerStatusCache(restoredStatuses, cachedTimestamp, remoteInstanceUris, transportProtocol);
        }

        return null;
    }

    private Set<PeerStatus> readPeerStatuses(final BufferedReader reader) throws IOException {
        final Set<PeerStatus> statuses = new HashSet<>();
        String line;
        while ((line = reader.readLine()) != null) {
            final String[] splits = line.split(Pattern.quote(":"));
            if (splits.length != 3 && splits.length != 4) {
                continue;
            }

            final String hostname = splits[0];
            final int port = Integer.parseInt(splits[1]);
            final boolean secure = Boolean.parseBoolean(splits[2]);

            final boolean supportQueryForPeer = splits.length == 4 && Boolean.parseBoolean(splits[3]);

            statuses.add(new PeerStatus(new PeerDescription(hostname, port, secure), 1, supportQueryForPeer));
        }

        return statuses;
    }


    @FunctionalInterface
    protected interface IOConsumer<T> {
        void accept(T value) throws IOException;
    }

    protected void write(final PeerStatusCache peerStatusCache, final IOConsumer<String> consumer) throws IOException {
        consumer.accept(peerStatusCache.getTransportProtocol().name() + "\n");
        consumer.accept(REMOTE_INSTANCE_URIS_PREFIX + peerStatusCache.getRemoteInstanceUris() + "\n");
        for (final PeerStatus status : peerStatusCache.getStatuses()) {
            final PeerDescription description = status.getPeerDescription();
            final String line = description.getHostname() + ":" + description.getPort() + ":" + description.isSecure() + ":" + status.isQueryForPeers() + "\n";
            consumer.accept(line);
        }
    }

}
