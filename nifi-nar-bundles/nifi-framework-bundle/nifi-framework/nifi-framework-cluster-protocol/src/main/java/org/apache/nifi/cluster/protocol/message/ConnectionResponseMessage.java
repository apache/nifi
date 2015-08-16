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

import javax.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.cluster.protocol.ConnectionResponse;

@XmlRootElement(name = "connectionResponseMessage")
public class ConnectionResponseMessage extends ProtocolMessage {

    private ConnectionResponse connectionResponse;
    private String clusterManagerDN;

    public ConnectionResponseMessage() {
    }

    public ConnectionResponse getConnectionResponse() {
        return connectionResponse;
    }

    public void setConnectionResponse(final ConnectionResponse connectionResponse) {
        this.connectionResponse = connectionResponse;

        if (clusterManagerDN != null) {
            this.connectionResponse.setClusterManagerDN(clusterManagerDN);
        }
    }

    public void setClusterManagerDN(final String dn) {
        if (connectionResponse != null) {
            connectionResponse.setClusterManagerDN(dn);
        }
        this.clusterManagerDN = dn;
    }

    /**
     * @return the DN of the NCM, if it is available or <code>null</code>
     * otherwise
     */
    public String getClusterManagerDN() {
        return clusterManagerDN;
    }

    @Override
    public MessageType getType() {
        return MessageType.CONNECTION_RESPONSE;
    }

}
