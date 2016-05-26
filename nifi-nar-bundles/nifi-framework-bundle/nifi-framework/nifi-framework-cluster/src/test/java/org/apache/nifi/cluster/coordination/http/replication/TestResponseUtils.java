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

package org.apache.nifi.cluster.coordination.http.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.junit.Test;
import org.mockito.Mockito;

import com.sun.jersey.api.client.ClientResponse;

public class TestResponseUtils {

    @Test
    public void testFindLongResponseTimes() throws URISyntaxException {
        final Map<NodeIdentifier, NodeResponse> responses = new HashMap<>();
        final NodeIdentifier id1 = new NodeIdentifier("1", "localhost", 8000, "localhost", 8001, "localhost", 8002, false);
        final NodeIdentifier id2 = new NodeIdentifier("2", "localhost", 8200, "localhost", 8201, "localhost", 8202, false);
        final NodeIdentifier id3 = new NodeIdentifier("3", "localhost", 8300, "localhost", 8301, "localhost", 8302, false);
        final NodeIdentifier id4 = new NodeIdentifier("4", "localhost", 8400, "localhost", 8401, "localhost", 8402, false);

        final URI uri = new URI("localhost:8080");
        final ClientResponse clientResponse = Mockito.mock(ClientResponse.class);
        responses.put(id1, new NodeResponse(id1, "GET", uri, clientResponse, TimeUnit.MILLISECONDS.toNanos(80), "1"));
        responses.put(id2, new NodeResponse(id1, "GET", uri, clientResponse, TimeUnit.MILLISECONDS.toNanos(92), "1"));
        responses.put(id3, new NodeResponse(id1, "GET", uri, clientResponse, TimeUnit.MILLISECONDS.toNanos(3), "1"));
        responses.put(id4, new NodeResponse(id1, "GET", uri, clientResponse, TimeUnit.MILLISECONDS.toNanos(120), "1"));

        final AsyncClusterResponse response = new AsyncClusterResponse() {
            @Override
            public String getRequestIdentifier() {
                return "1";
            }

            @Override
            public String getMethod() {
                return "GET";
            }

            @Override
            public String getURIPath() {
                return null;
            }

            @Override
            public Set<NodeIdentifier> getNodesInvolved() {
                return new HashSet<>(responses.keySet());
            }

            @Override
            public Set<NodeIdentifier> getCompletedNodeIdentifiers() {
                return getNodesInvolved();
            }

            @Override
            public boolean isComplete() {
                return true;
            }

            @Override
            public boolean isOlderThan(long time, TimeUnit timeUnit) {
                return true;
            }

            @Override
            public NodeResponse getMergedResponse() {
                return null;
            }

            @Override
            public NodeResponse awaitMergedResponse() throws InterruptedException {
                return null;
            }

            @Override
            public NodeResponse awaitMergedResponse(long timeout, TimeUnit timeUnit) throws InterruptedException {
                return null;
            }

            @Override
            public NodeResponse getNodeResponse(NodeIdentifier nodeId) {
                return responses.get(nodeId);
            }

            @Override
            public Set<NodeResponse> getCompletedNodeResponses() {
                return new HashSet<>(responses.values());
            }
        };

        Set<NodeIdentifier> slowResponses = ResponseUtils.findLongResponseTimes(response, 1.5D);
        assertTrue(slowResponses.isEmpty());

        responses.put(id4, new NodeResponse(id1, "GET", uri, clientResponse, TimeUnit.MILLISECONDS.toNanos(2500), "1"));
        slowResponses = ResponseUtils.findLongResponseTimes(response, 1.5D);
        assertEquals(1, slowResponses.size());
        assertEquals(id4, slowResponses.iterator().next());
    }

}
