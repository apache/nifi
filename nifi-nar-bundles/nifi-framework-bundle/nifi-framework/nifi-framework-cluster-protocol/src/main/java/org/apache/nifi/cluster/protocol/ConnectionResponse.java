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

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.jaxb.message.ConnectionResponseAdapter;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    private final DataFlow dataFlow;
    private final String instanceId;
    private final List<NodeConnectionStatus> nodeStatuses;
    private final List<ComponentRevision> componentRevisions;


    public ConnectionResponse(final NodeIdentifier nodeIdentifier, final DataFlow dataFlow,
        final String instanceId, final List<NodeConnectionStatus> nodeStatuses, final List<ComponentRevision> componentRevisions) {

        if (nodeIdentifier == null) {
            throw new IllegalArgumentException("Node identifier may not be empty or null.");
        }

        this.nodeIdentifier = nodeIdentifier;
        this.dataFlow = dataFlow;
        this.tryLaterSeconds = 0;
        this.rejectionReason = null;
        this.instanceId = instanceId;
        this.nodeStatuses = Collections.unmodifiableList(new ArrayList<>(nodeStatuses));
        this.componentRevisions = componentRevisions == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(componentRevisions));
    }

    public ConnectionResponse(final int tryLaterSeconds, final String explanation) {
        if (tryLaterSeconds <= 0) {
            throw new IllegalArgumentException("Try-Later seconds must be nonnegative: " + tryLaterSeconds);
        }
        this.dataFlow = null;
        this.nodeIdentifier = null;
        this.tryLaterSeconds = tryLaterSeconds;
        this.rejectionReason = explanation;
        this.instanceId = null;
        this.nodeStatuses = null;
        this.componentRevisions = null;
    }

    private ConnectionResponse(final String rejectionReason) {
        this.dataFlow = null;
        this.nodeIdentifier = null;
        this.tryLaterSeconds = 0;
        this.rejectionReason = rejectionReason;
        this.instanceId = null;
        this.nodeStatuses = null;
        this.componentRevisions = null;
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

    public DataFlow getDataFlow() {
        return dataFlow;
    }

    public NodeIdentifier getNodeIdentifier() {
        return nodeIdentifier;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public List<NodeConnectionStatus> getNodeConnectionStatuses() {
        return nodeStatuses;
    }

    public List<ComponentRevision> getComponentRevisions() {
        return componentRevisions;
    }
}
