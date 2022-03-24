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

package org.apache.nifi.cluster.protocol.message;

import org.apache.nifi.cluster.coordination.node.NodeWorkload;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

@XmlRootElement(name = "clusterWorkloadResponse")
public class ClusterWorkloadResponseMessage extends ProtocolMessage {

    private Map<NodeIdentifier, NodeWorkload> nodeWorkloads;

    @Override
    public MessageType getType() {
        return MessageType.CLUSTER_WORKLOAD_RESPONSE;
    }

    public Map<NodeIdentifier, NodeWorkload> getNodeWorkloads() {
        return nodeWorkloads;
    }

    public void setNodeWorkloads(Map<NodeIdentifier, NodeWorkload> nodeWorkloads) {
        this.nodeWorkloads = nodeWorkloads;
    }
}
