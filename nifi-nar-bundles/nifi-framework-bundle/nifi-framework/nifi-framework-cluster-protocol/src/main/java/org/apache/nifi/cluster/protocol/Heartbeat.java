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

import java.util.Date;

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.jaxb.message.HeartbeatAdapter;

/**
 * A heartbeat for indicating the status of a node to the cluster.
 *
 */
@XmlJavaTypeAdapter(HeartbeatAdapter.class)
public class Heartbeat {

    private final NodeIdentifier nodeIdentifier;
    private final NodeConnectionStatus connectionStatus;
    private final long createdTimestamp;
    private final byte[] payload;

    public Heartbeat(final NodeIdentifier nodeIdentifier, final NodeConnectionStatus connectionStatus, final byte[] payload) {
        if (nodeIdentifier == null) {
            throw new IllegalArgumentException("Node Identifier may not be null.");
        }
        this.nodeIdentifier = nodeIdentifier;
        this.connectionStatus = connectionStatus;
        this.payload = payload;
        this.createdTimestamp = new Date().getTime();
    }

    public NodeIdentifier getNodeIdentifier() {
        return nodeIdentifier;
    }

    public byte[] getPayload() {
        return payload;
    }

    public NodeConnectionStatus getConnectionStatus() {
        return connectionStatus;
    }

    @XmlTransient
    public long getCreatedTimestamp() {
        return createdTimestamp;
    }
}
