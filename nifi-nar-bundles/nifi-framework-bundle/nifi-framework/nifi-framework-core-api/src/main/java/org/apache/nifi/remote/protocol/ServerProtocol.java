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
package org.apache.nifi.remote.protocol;

import java.io.IOException;
import java.util.Optional;

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.VersionedRemoteResource;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;

public interface ServerProtocol extends VersionedRemoteResource {

    /**
     *
     * @param rootGroup group
     */
    void setRootProcessGroup(ProcessGroup rootGroup);

    RootGroupPort getPort();

    /**
     * Optional operation. Sets the NodeInformant to use in this Protocol, if a
     * NodeInformant is supported. Otherwise, throws
     * UnsupportedOperationException
     *
     * @param nodeInformant informant
     */
    void setNodeInformant(NodeInformant nodeInformant);

    /**
     * Receives the handshake from the Peer
     *
     * @param peer peer
     * @throws IOException ioe
     * @throws HandshakeException he
     */
    void handshake(Peer peer) throws IOException, HandshakeException;

    /**
     * @return <code>true</code> if the handshaking process was completed
     * successfully, <code>false</code> if either the handshaking process has
     * not happened or the handshake failed
     */
    boolean isHandshakeSuccessful();

    /**
     * Negotiates the FlowFileCodec that is to be used for transferring
     * FlowFiles
     *
     * @param peer peer
     * @return the codec to use
     * @throws IOException ioe
     * @throws org.apache.nifi.remote.exception.ProtocolException pe
     */
    FlowFileCodec negotiateCodec(Peer peer) throws IOException, ProtocolException;

    /**
     * @return the codec that has already been negotiated by this Protocol, if
     * any
     */
    FlowFileCodec getPreNegotiatedCodec();

    /**
     * Reads the Request Type of the next request from the Peer
     *
     * @param peer peer
     * @return the RequestType that the peer would like to happen - or null, if
     * no data available
     * @throws java.io.IOException ioe
     */
    RequestType getRequestType(Peer peer) throws IOException;

    /**
     * Sends FlowFiles to the specified peer
     *
     * @param peer peer
     * @param context context
     * @param session session
     * @param codec codec
     *
     * @return the number of FlowFiles transferred
     * @throws java.io.IOException ioe
     * @throws org.apache.nifi.remote.exception.ProtocolException pe
     */
    int transferFlowFiles(Peer peer, ProcessContext context, ProcessSession session, FlowFileCodec codec) throws IOException, ProtocolException;

    /**
     * Receives FlowFiles from the specified peer
     *
     * @param peer peer
     * @param context context
     * @param session session
     * @param codec codec
     * @throws IOException ioe
     *
     * @return the number of FlowFiles received
     * @throws ProtocolException pe
     */
    int receiveFlowFiles(Peer peer, ProcessContext context, ProcessSession session, FlowFileCodec codec) throws IOException, ProtocolException;

    /**
     * @return the number of milliseconds after a request is received for which
     * the request is still valid. A valid of 0 indicates that the request will
     * not expire
     */
    long getRequestExpiration();

    /**
     * Sends a list of all nodes in the cluster to the specified peer. If not in
     * a cluster, sends info about itself
     *
     * @param peer peer
     * @param clusterNodeInfo the cluster information
     * @param self the node which received the request
     *
     * @throws java.io.IOException ioe
     */
    void sendPeerList(
            Peer peer,
            Optional<ClusterNodeInformation> clusterNodeInfo,
            NodeInformation self) throws IOException;

    void shutdown(Peer peer);

    boolean isShutdown();
}
