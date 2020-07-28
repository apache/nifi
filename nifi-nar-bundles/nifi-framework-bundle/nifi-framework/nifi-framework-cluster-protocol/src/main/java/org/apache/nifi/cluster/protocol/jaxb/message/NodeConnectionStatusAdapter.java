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

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;

public class NodeConnectionStatusAdapter extends XmlAdapter<AdaptedNodeConnectionStatus, NodeConnectionStatus> {

    @Override
    public NodeConnectionStatus unmarshal(final AdaptedNodeConnectionStatus adapted) throws Exception {
        return new NodeConnectionStatus(adapted.getUpdateId(),
            adapted.getNodeId(),
            adapted.getState(),
            adapted.getOffloadCode(),
            adapted.getDisconnectCode(),
            adapted.getReason(),
            adapted.getConnectionRequestTime());
    }

    @Override
    public AdaptedNodeConnectionStatus marshal(final NodeConnectionStatus toAdapt) throws Exception {
        final AdaptedNodeConnectionStatus adapted = new AdaptedNodeConnectionStatus();
        if (toAdapt != null) {
            adapted.setUpdateId(toAdapt.getUpdateIdentifier());
            adapted.setNodeId(toAdapt.getNodeIdentifier());
            adapted.setConnectionRequestTime(toAdapt.getConnectionRequestTime());
            adapted.setOffloadCode(toAdapt.getOffloadCode());
            adapted.setDisconnectCode(toAdapt.getDisconnectCode());
            adapted.setReason(toAdapt.getReason());
            adapted.setState(toAdapt.getState());
        }
        return adapted;
    }
}
