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

/**
 * Wraps a protocol message and an identifier for sending the message by way
 * multicast. The identifier is necessary for the sender to identify a message
 * sent by it.
 *
 */
@XmlRootElement(name = "multicastMessage")
public class MulticastProtocolMessage extends ProtocolMessage {

    private ProtocolMessage protocolMessage;

    private String id;

    public MulticastProtocolMessage() {
    }

    public MulticastProtocolMessage(final String id, final ProtocolMessage protocolMessage) {
        this.protocolMessage = protocolMessage;
        this.id = id;
    }

    @Override
    public MessageType getType() {
        if (protocolMessage == null) {
            return null;
        }
        return protocolMessage.getType();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public ProtocolMessage getProtocolMessage() {
        return protocolMessage;
    }

    public void setProtocolMessage(ProtocolMessage protocolMessage) {
        this.protocolMessage = protocolMessage;
    }

}
