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
package org.apache.nifi.cluster.manager.impl;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.node.Node.Status;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class HttpResponseMapperImplTest {

    private HttpResponseMapperImpl mapper;

    private URI dummyUri;

    @Before
    public void setup() throws URISyntaxException {
        mapper = new HttpResponseMapperImpl();
        dummyUri = new URI("http://dummy.com");
    }

    @Test
    public void testToNodeStatusWithNo2xxResponses() {

        Set<NodeResponse> nodeResponses = new HashSet<>();
        nodeResponses.add(createNodeResourceResponse("1", 400));
        nodeResponses.add(createNodeResourceResponse("2", 100));
        nodeResponses.add(createNodeResourceResponse("3", 300));
        nodeResponses.add(createNodeResourceResponse("4", 500));

        Map<NodeResponse, Status> map = mapper.map(dummyUri, nodeResponses);

        // since no 2xx responses, any 5xx is disconnected
        for (Map.Entry<NodeResponse, Status> entry : map.entrySet()) {
            NodeResponse response = entry.getKey();
            Status status = entry.getValue();
            switch (response.getNodeId().getId()) {
                case "1":
                    assertTrue(status == Node.Status.CONNECTED);
                    break;
                case "2":
                    assertTrue(status == Node.Status.CONNECTED);
                    break;
                case "3":
                    assertTrue(status == Node.Status.CONNECTED);
                    break;
                case "4":
                    assertTrue(status == Node.Status.DISCONNECTED);
                    break;
            }
        }
    }

    @Test
    public void testToNodeStatusWith2xxResponses() {

        Set<NodeResponse> nodeResponses = new HashSet<>();
        nodeResponses.add(createNodeResourceResponse("1", 200));
        nodeResponses.add(createNodeResourceResponse("2", 100));
        nodeResponses.add(createNodeResourceResponse("3", 300));
        nodeResponses.add(createNodeResourceResponse("4", 500));

        Map<NodeResponse, Status> map = mapper.map(dummyUri, nodeResponses);

        // since there were 2xx responses, any non-2xx is disconnected
        for (Map.Entry<NodeResponse, Status> entry : map.entrySet()) {
            NodeResponse response = entry.getKey();
            Status status = entry.getValue();
            switch (response.getNodeId().getId()) {
                case "1":
                    assertTrue(status == Node.Status.CONNECTED);
                    break;
                case "2":
                    assertTrue(status == Node.Status.DISCONNECTED);
                    break;
                case "3":
                    assertTrue(status == Node.Status.DISCONNECTED);
                    break;
                case "4":
                    assertTrue(status == Node.Status.DISCONNECTED);
                    break;
            }
        }
    }

    private NodeResponse createNodeResourceResponse(String nodeId, int statusCode) {

        ClientResponse clientResponse = mock(ClientResponse.class);
        when(clientResponse.getStatus()).thenReturn(statusCode);
        when(clientResponse.getHeaders()).thenReturn(new MultivaluedMapImpl());
        when(clientResponse.getEntityInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

        NodeIdentifier nodeIdentifier = new NodeIdentifier(nodeId, "localhost", 1, "localhost", 1);
        return new NodeResponse(nodeIdentifier, "GET", dummyUri, clientResponse, 1L, "111");
    }
}
