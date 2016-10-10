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
package org.apache.nifi.cluster.manager.exception;

import org.apache.nifi.cluster.exception.ClusterException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 *
 */
public class BlockedByFirewallException extends ClusterException {
    private static final long serialVersionUID = 1L;
    private final NodeIdentifier nodeId;
    private final boolean isExistingNode;

    public BlockedByFirewallException(NodeIdentifier nodeId, boolean isExistingNode, String msg, Throwable cause) {
        super(msg, cause);
        this.nodeId = nodeId;
        this.isExistingNode = isExistingNode;
    }

    public BlockedByFirewallException(NodeIdentifier nodeId, boolean isExistingNode, Throwable cause) {
        super(cause);
        this.nodeId = nodeId;
        this.isExistingNode = isExistingNode;
    }

    public BlockedByFirewallException(NodeIdentifier nodeId, boolean isExistingNode, String msg) {
        super(msg);
        this.nodeId = nodeId;
        this.isExistingNode = isExistingNode;
    }

    public BlockedByFirewallException(NodeIdentifier nodeId, boolean isExistingNode) {
        this.nodeId = nodeId;
        this.isExistingNode = isExistingNode;
    }

    public NodeIdentifier getNodeId() {
        return nodeId;
    }

    public boolean isExistingNode() {
        return isExistingNode;
    }

}
