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
import java.util.Set;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.VersionedRemoteResource;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;

public interface ClientProtocol extends VersionedRemoteResource {

    void handshake(Peer peer) throws IOException, HandshakeException, UnknownPortException, PortNotRunningException;

    Set<PeerStatus> getPeerStatuses(Peer currentPeer) throws IOException, ProtocolException;

    FlowFileCodec negotiateCodec(Peer peer) throws IOException, ProtocolException;

    int receiveFlowFiles(Peer peer, ProcessContext context, ProcessSession session, FlowFileCodec codec) throws IOException, ProtocolException;

    int transferFlowFiles(Peer peer, ProcessContext context, ProcessSession session, FlowFileCodec codec) throws IOException, ProtocolException;

    void shutdown(Peer peer) throws IOException, ProtocolException;

    boolean isReadyForFileTransfer();

    Transaction startTransaction(Peer peer, FlowFileCodec codec, TransferDirection direction) throws IOException;

    /**
     * @return <code>true</code> if remote instance indicates that the port is
     * invalid
     * @throws IllegalStateException if a handshake has not successfully
     * completed
     */
    boolean isPortInvalid() throws IllegalStateException;

    /**
     * @return <code>true</code> if remote instance indicates that the port is
     * unknown
     * @throws IllegalStateException if a handshake has not successfully
     * completed
     */
    boolean isPortUnknown();

    /**
     * @return <code>true</code> if remote instance indicates that the port's
     * destination is full
     * @throws IllegalStateException if a handshake has not successfully
     * completed
     */
    boolean isDestinationFull();
}
