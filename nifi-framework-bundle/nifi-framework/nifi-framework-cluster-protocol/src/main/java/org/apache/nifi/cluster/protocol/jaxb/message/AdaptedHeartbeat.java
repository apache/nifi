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
package org.apache.nifi.cluster.protocol.jaxb.message;

import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 */
public class AdaptedHeartbeat {

    private NodeIdentifier nodeIdentifier;
    private byte[] payload;
    private NodeConnectionStatus connectionStatus;

    @XmlJavaTypeAdapter(NodeIdentifierAdapter.class)
    public NodeIdentifier getNodeIdentifier() {
        return nodeIdentifier;
    }

    public void setNodeIdentifier(NodeIdentifier nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    public void setConnectionStatus(NodeConnectionStatus connectionStatus) {
        this.connectionStatus = connectionStatus;
    }

    public NodeConnectionStatus getConnectionStatus() {
        return connectionStatus;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
