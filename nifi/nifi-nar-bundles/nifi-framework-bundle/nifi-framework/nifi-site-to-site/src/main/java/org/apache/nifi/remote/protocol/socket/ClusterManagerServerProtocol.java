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
package org.apache.nifi.remote.protocol.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManagerServerProtocol implements ServerProtocol {
    public static final String RESOURCE_NAME = "SocketFlowFileProtocol";

    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(1);
    private final Logger logger = LoggerFactory.getLogger(ClusterManagerServerProtocol.class);
    private NodeInformant nodeInformant;
    
    private String commsIdentifier;
    private boolean shutdown = false;
    private boolean handshakeCompleted = false;
    private long requestExpirationMillis = 30000L;

    public ClusterManagerServerProtocol() {
    }
    
    
    @Override
    public void setNodeInformant(final NodeInformant nodeInformant) {
        this.nodeInformant = nodeInformant;
    }
    
    @Override
    public void handshake(final Peer peer) throws IOException, HandshakeException {
        if ( handshakeCompleted ) {
            throw new IllegalStateException("Handshake has already been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        
        // read communications identifier
        commsIdentifier = dis.readUTF();
        
        // read all of the properties. we don't really care what the properties are.
        final int numProperties = dis.readInt();
        for (int i=0; i < numProperties; i++) {
            final String propertyName = dis.readUTF();
            final String propertyValue = dis.readUTF();
            
            final HandshakeProperty property;
            try {
                property = HandshakeProperty.valueOf(propertyName);
                if ( HandshakeProperty.REQUEST_EXPIRATION_MILLIS.equals(property) ) {
                    requestExpirationMillis = Long.parseLong(propertyValue);
                }
            } catch (final Exception e) {
            }
        }
        
        // send "OK" response
        ResponseCode.PROPERTIES_OK.writeResponse(dos);
        
        logger.debug("Successfully completed handshake with {}; CommsID={}", peer, commsIdentifier);
        handshakeCompleted = true;
    }
    
    @Override
    public boolean isHandshakeSuccessful() {
        return handshakeCompleted;
    }

    @Override
    public void sendPeerList(final Peer peer) throws IOException {
        if ( !handshakeCompleted ) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        final ClusterNodeInformation clusterNodeInfo = nodeInformant.getNodeInformation();
        final Collection<NodeInformation> nodeInfos = clusterNodeInfo.getNodeInformation();
        
        // determine how many nodes have Site-to-site enabled
        int numPeers = 0;
        for ( final NodeInformation nodeInfo : nodeInfos ) {
            if (nodeInfo.getSiteToSitePort() != null) {
                numPeers++;
            }
        }
        
        dos.writeInt(numPeers);
        for ( final NodeInformation nodeInfo : nodeInfos ) {
            if ( nodeInfo.getSiteToSitePort() == null ) {
                continue;
            }
            
            dos.writeUTF(nodeInfo.getHostname());
            dos.writeInt(nodeInfo.getSiteToSitePort());
            dos.writeBoolean(nodeInfo.isSiteToSiteSecure());
            dos.writeInt(nodeInfo.getTotalFlowFiles());
        }
        
        logger.info("Redirected {} to {} nodes", peer, numPeers);
        
        dos.flush();
    }

    @Override
    public void shutdown(final Peer peer) {
        shutdown = true;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }
    
    @Override
    public FlowFileCodec negotiateCodec(Peer peer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return null;
    }

    @Override
    public RequestType getRequestType(final Peer peer) throws IOException {
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        return RequestType.readRequestType(dis);
    }

    @Override
    public int transferFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int receiveFlowFiles(Peer peer, ProcessContext context, ProcessSession session, FlowFileCodec codec) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public String getResourceName() {
        return RESOURCE_NAME;
    }

    @Override
    public void setRootProcessGroup(final ProcessGroup rootGroup) {
    }

    @Override
    public RootGroupPort getPort() {
        return null;
    }

    @Override
    public long getRequestExpiration() {
        return requestExpirationMillis;
    }
}
