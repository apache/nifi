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

import org.apache.nifi.cluster.NodeInformant;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.VersionedRemoteResource;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;

public interface ServerProtocol extends VersionedRemoteResource {

    /**
     *
     * @param rootGroup
     */
    void setRootProcessGroup(ProcessGroup rootGroup);

    RootGroupPort getPort();

    /**
     * Optional operation. Sets the NodeInformant to use in this Protocol, if a
     * NodeInformant is supported. Otherwise, throws
     * UnsupportedOperationException
     *
     * @param nodeInformant
     */
    void setNodeInformant(NodeInformant nodeInformant);

    /**
     * Receives the handshake from the Peer
     *
     * @param peer
     * @throws IOException
     * @throws HandshakeException
     */
    void handshake(Peer peer) throws IOException, HandshakeException;

    /**
     * Returns <code>true</code> if the handshaking process was completed
     * successfully, <code>false</code> if either the handshaking process has
     * not happened or the handshake failed
     *
     * @return
     */
    boolean isHandshakeSuccessful();

    /**
     * Negotiates the FlowFileCodec that is to be used for transferring
     * FlowFiles
     *
     * @param peer
     * @return
     * @throws IOException
     * @throws BadRequestException
     */
    FlowFileCodec negotiateCodec(Peer peer) throws IOException, ProtocolException;

    /**
     * Returns the codec that has already been negotiated by this Protocol, if
     * any.
     *
     * @return
     */
    FlowFileCodec getPreNegotiatedCodec();

    /**
     * Reads the Request Type of the next request from the Peer
     *
     * @return the RequestType that the peer would like to happen - or null, if
     * no data available
     */
    RequestType getRequestType(Peer peer) throws IOException;

    /**
     * Sends FlowFiles to the specified peer
     *
     * @param peer
     * @param context
     * @param session
     * @param codec
     *
     * @return the number of FlowFiles transferred
     */
    int transferFlowFiles(Peer peer, ProcessContext context, ProcessSession session, FlowFileCodec codec) throws IOException, ProtocolException;

    /**
     * Receives FlowFiles from the specified peer
     *
     * @param peer
     * @param context
     * @param session
     * @param codec
     * @throws IOException
     *
     * @return the number of FlowFiles received
     * @throws ProtocolException
     */
    int receiveFlowFiles(Peer peer, ProcessContext context, ProcessSession session, FlowFileCodec codec) throws IOException, ProtocolException;

    /**
     * Returns the number of milliseconds after a request is received for which
     * the request is still valid. A valid of 0 indicates that the request will
     * not expire.
     *
     * @return
     */
    long getRequestExpiration();

    /**
     * Sends a list of all nodes in the cluster to the specified peer. If not in
     * a cluster, sends info about itself
     *
     * @param peer
     */
    void sendPeerList(Peer peer) throws IOException;

    void shutdown(Peer peer);

    boolean isShutdown();
}
