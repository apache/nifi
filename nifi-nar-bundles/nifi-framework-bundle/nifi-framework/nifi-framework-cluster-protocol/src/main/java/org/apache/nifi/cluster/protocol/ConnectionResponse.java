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
package org.apache.nifi.cluster.protocol;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.protocol.jaxb.message.ConnectionResponseAdapter;

/**
 * The cluster manager's response to a node's connection request. If the manager
 * has a current copy of the data flow, then it is returned with a node
 * identifier to the node. Otherwise, the manager will provide a "try again in X
 * seconds" response to the node in hopes that a current data flow will be
 * available upon subsequent requests.
 *
 */
@XmlJavaTypeAdapter(ConnectionResponseAdapter.class)
public class ConnectionResponse {

    private final String rejectionReason;
    private final int tryLaterSeconds;
    private final NodeIdentifier nodeIdentifier;
    private final StandardDataFlow dataFlow;
    private final Integer managerRemoteInputPort;
    private final Boolean managerRemoteCommsSecure;
    private final String instanceId;

    private volatile String clusterManagerDN;

    public ConnectionResponse(final NodeIdentifier nodeIdentifier, final StandardDataFlow dataFlow,
        final Integer managerRemoteInputPort, final Boolean managerRemoteCommsSecure, final String instanceId) {
        if (nodeIdentifier == null) {
            throw new IllegalArgumentException("Node identifier may not be empty or null.");
        } else if (dataFlow == null) {
            throw new IllegalArgumentException("DataFlow may not be null.");
        }
        this.nodeIdentifier = nodeIdentifier;
        this.dataFlow = dataFlow;
        this.tryLaterSeconds = 0;
        this.rejectionReason = null;
        this.managerRemoteInputPort = managerRemoteInputPort;
        this.managerRemoteCommsSecure = managerRemoteCommsSecure;
        this.instanceId = instanceId;
    }

    public ConnectionResponse(final int tryLaterSeconds) {
        if (tryLaterSeconds <= 0) {
            throw new IllegalArgumentException("Try-Later seconds may not be nonnegative: " + tryLaterSeconds);
        }
        this.dataFlow = null;
        this.nodeIdentifier = null;
        this.tryLaterSeconds = tryLaterSeconds;
        this.rejectionReason = null;
        this.managerRemoteInputPort = null;
        this.managerRemoteCommsSecure = null;
        this.instanceId = null;
    }

    private ConnectionResponse(final String rejectionReason) {
        this.dataFlow = null;
        this.nodeIdentifier = null;
        this.tryLaterSeconds = 0;
        this.rejectionReason = rejectionReason;
        this.managerRemoteInputPort = null;
        this.managerRemoteCommsSecure = null;
        this.instanceId = null;
    }

    public static ConnectionResponse createBlockedByFirewallResponse() {
        return new ConnectionResponse("Blocked by Firewall");
    }

    public static ConnectionResponse createConflictingNodeIdResponse(final String otherNode) {
        return new ConnectionResponse("The Node Identifier provided already belongs to node " + otherNode);
    }

    public static ConnectionResponse createRejectionResponse(final String explanation) {
        return new ConnectionResponse(explanation);
    }

    public boolean shouldTryLater() {
        return tryLaterSeconds > 0;
    }

    public String getRejectionReason() {
        return rejectionReason;
    }

    public int getTryLaterSeconds() {
        return tryLaterSeconds;
    }

    public StandardDataFlow getDataFlow() {
        return dataFlow;
    }

    public NodeIdentifier getNodeIdentifier() {
        return nodeIdentifier;
    }

    public Integer getManagerRemoteInputPort() {
        return managerRemoteInputPort;
    }

    public Boolean isManagerRemoteCommsSecure() {
        return managerRemoteCommsSecure;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setClusterManagerDN(final String dn) {
        this.clusterManagerDN = dn;
    }

    /**
     * @return the DN of the NCM, if it is available or <code>null</code>
     * otherwise
     */
    public String getClusterManagerDN() {
        return clusterManagerDN;
    }
}
