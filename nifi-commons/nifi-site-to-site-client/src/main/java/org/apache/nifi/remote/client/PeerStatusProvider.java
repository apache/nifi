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

import java.io.IOException;
import java.util.Set;

/**
 * This interface defines methods used from {@link PeerSelector}.
 */
public interface PeerStatusProvider {

    /**
     * <p>
     * Returns a PeerDescription instance, which represents a bootstrap remote NiFi node.
     * The bootstrap node is always used to fetch remote peer statuses.
     * </p>
     * <p>
     * Once the PeerSelector successfully got remote peer statuses, it periodically fetches remote peer statuses,
     * so that it can detect remote NiFi cluster topology changes such as addition or removal of nodes.
     * To refresh remote peer statuses, PeerSelector calls {@link #fetchRemotePeerStatuses} with one of query-able nodes
     * lastly fetched from the remote NiFi cluster, until it gets a successful result,
     * or throws IOException if none of them responds successfully.
     * </p>
     * <p>
     * This mechanism lets PeerSelector works even if the bootstrap remote NiFi node goes down.
     * </p>
     * @return peer description of a bootstrap remote NiFi node
     * @throws IOException thrown when it fails to retrieve the bootstrap remote node information
     */
    PeerDescription getBootstrapPeerDescription() throws IOException;

    /**
     * Fetch peer statuses from a remote NiFi cluster.
     * Implementation of this method should fetch peer statuses from the node
     * represented by the passed PeerDescription using its transport protocol.
     * @param peerDescription a bootstrap node or one of query-able nodes lastly fetched successfully
     * @return Remote peer statuses
     * @throws IOException thrown when it fails to fetch peer statuses of the remote cluster from the specified peer
     */
    Set<PeerStatus> fetchRemotePeerStatuses(final PeerDescription peerDescription) throws IOException;

}
