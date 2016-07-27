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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.ComponentRevision;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.NodeConnectionStatusRequestMessage;
import org.apache.nifi.cluster.protocol.message.NodeConnectionStatusResponseMessage;
import org.apache.nifi.web.Revision;
import org.junit.Test;

public class TestJaxbProtocolUtils {

    @Test
    public void testRoundTripConnectionResponse() throws JAXBException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final ConnectionResponseMessage msg = new ConnectionResponseMessage();
        final NodeIdentifier nodeId = new NodeIdentifier("id", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, true);
        final DataFlow dataFlow = new StandardDataFlow(new byte[0], new byte[0], new byte[0]);
        final List<NodeConnectionStatus> nodeStatuses = Collections.singletonList(new NodeConnectionStatus(nodeId, DisconnectionCode.NOT_YET_CONNECTED));
        final List<ComponentRevision> componentRevisions = Collections.singletonList(ComponentRevision.fromRevision(new Revision(8L, "client-1", "component-1")));
        msg.setConnectionResponse(new ConnectionResponse(nodeId, dataFlow, "instance-1", nodeStatuses, componentRevisions));

        JaxbProtocolUtils.JAXB_CONTEXT.createMarshaller().marshal(msg, baos);
        final Object unmarshalled = JaxbProtocolUtils.JAXB_CONTEXT.createUnmarshaller().unmarshal(new ByteArrayInputStream(baos.toByteArray()));
        assertTrue(unmarshalled instanceof ConnectionResponseMessage);
        final ConnectionResponseMessage unmarshalledMsg = (ConnectionResponseMessage) unmarshalled;

        final List<ComponentRevision> revisions = msg.getConnectionResponse().getComponentRevisions();
        assertEquals(1, revisions.size());
        assertEquals(8L, revisions.get(0).getVersion().longValue());
        assertEquals("client-1", revisions.get(0).getClientId());
        assertEquals("component-1", revisions.get(0).getComponentId());

        assertEquals(revisions, unmarshalledMsg.getConnectionResponse().getComponentRevisions());
    }

    @Test
    public void testRoundTripConnectionStatusRequest() throws JAXBException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final NodeConnectionStatusRequestMessage msg = new NodeConnectionStatusRequestMessage();

        JaxbProtocolUtils.JAXB_CONTEXT.createMarshaller().marshal(msg, baos);
        final Object unmarshalled = JaxbProtocolUtils.JAXB_CONTEXT.createUnmarshaller().unmarshal(new ByteArrayInputStream(baos.toByteArray()));
        assertTrue(unmarshalled instanceof NodeConnectionStatusRequestMessage);
    }


    @Test
    public void testRoundTripConnectionStatusResponse() throws JAXBException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final NodeConnectionStatusResponseMessage msg = new NodeConnectionStatusResponseMessage();
        final NodeIdentifier nodeId = new NodeIdentifier("id", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, true);
        final NodeConnectionStatus nodeStatus = new NodeConnectionStatus(nodeId, DisconnectionCode.NOT_YET_CONNECTED);
        msg.setNodeConnectionStatus(nodeStatus);

        JaxbProtocolUtils.JAXB_CONTEXT.createMarshaller().marshal(msg, baos);
        final Object unmarshalled = JaxbProtocolUtils.JAXB_CONTEXT.createUnmarshaller().unmarshal(new ByteArrayInputStream(baos.toByteArray()));
        assertTrue(unmarshalled instanceof NodeConnectionStatusResponseMessage);
        final NodeConnectionStatusResponseMessage unmarshalledMsg = (NodeConnectionStatusResponseMessage) unmarshalled;

        final NodeConnectionStatus unmarshalledStatus = unmarshalledMsg.getNodeConnectionStatus();
        assertEquals(nodeStatus, unmarshalledStatus);
    }
}
