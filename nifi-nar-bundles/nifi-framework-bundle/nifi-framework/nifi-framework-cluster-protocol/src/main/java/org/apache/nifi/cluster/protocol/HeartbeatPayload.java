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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.security.xml.XmlUtils;

/**
 * The payload of the heartbeat. The payload contains status to inform the cluster manager the current workload of this node.
 *
 */
@XmlRootElement
public class HeartbeatPayload {

    private static final JAXBContext JAXB_CONTEXT;

    static {
        try {
            JAXB_CONTEXT = JAXBContext.newInstance(HeartbeatPayload.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private int activeThreadCount;
    private long totalFlowFileCount;
    private long totalFlowFileBytes;
    private long systemStartTime;
    private List<NodeConnectionStatus> clusterStatus;

    public int getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(final int activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    public long getTotalFlowFileCount() {
        return totalFlowFileCount;
    }

    public void setTotalFlowFileCount(final long totalFlowFileCount) {
        this.totalFlowFileCount = totalFlowFileCount;
    }

    public long getTotalFlowFileBytes() {
        return totalFlowFileBytes;
    }

    public void setTotalFlowFileBytes(final long totalFlowFileBytes) {
        this.totalFlowFileBytes = totalFlowFileBytes;
    }

    public long getSystemStartTime() {
        return systemStartTime;
    }

    public void setSystemStartTime(final long systemStartTime) {
        this.systemStartTime = systemStartTime;
    }

    public List<NodeConnectionStatus> getClusterStatus() {
        return clusterStatus;
    }

    public void setClusterStatus(final List<NodeConnectionStatus> clusterStatus) {
        this.clusterStatus = clusterStatus;
    }

    public byte[] marshal() throws ProtocolException {
        final ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
        marshal(this, payloadBytes);
        return payloadBytes.toByteArray();
    }

    public static void marshal(final HeartbeatPayload payload, final OutputStream os) throws ProtocolException {
        try {
            final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
            marshaller.marshal(payload, os);
        } catch (final JAXBException je) {
            throw new ProtocolException(je);
        }
    }

    public static HeartbeatPayload unmarshal(final InputStream is) throws ProtocolException {
        try {
            final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
            final XMLStreamReader xsr = XmlUtils.createSafeReader(is);
            return (HeartbeatPayload) unmarshaller.unmarshal(xsr);
        } catch (final JAXBException | XMLStreamException e) {
            throw new ProtocolException(e);
        }
    }

    public static HeartbeatPayload unmarshal(final byte[] bytes) throws ProtocolException {
        return unmarshal(new ByteArrayInputStream(bytes));
    }
}
