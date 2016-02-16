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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.testutils.HttpResponse;
import org.apache.nifi.cluster.manager.testutils.HttpResponseAction;
import org.apache.nifi.cluster.manager.testutils.HttpServer;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.Client;

/**
 */
public class HttpRequestReplicatorImplTest {

    private Client client;
    private HttpRequestReplicatorImpl replicator;
    private int executorThreadCount;
    private int serverThreadCount;
    private int serverPort;
    private HttpServer server;
    private Map<String, List<String>> expectedRequestParameters;
    private Map<String, String> expectedRequestHeaders;
    private Map<String, String> expectedResponseHeaders;
    private Object expectedEntity;
    private String expectedBody;
    private URI prototypeUri;

    @Before
    public void setUp() throws IOException, URISyntaxException {

        executorThreadCount = 5;
        serverThreadCount = 3;

        client = Client.create();

        replicator = new HttpRequestReplicatorImpl(executorThreadCount, client, "1 sec", "1 sec");
        replicator.start();

        expectedRequestHeaders = new HashMap<>();
        expectedRequestHeaders.put("header1", "header value1");
        expectedRequestHeaders.put("header2", "header value2");

        expectedRequestParameters = new HashMap<>();
        expectedRequestParameters.put("param1", Arrays.asList("p value1"));
        expectedRequestParameters.put("param2", Arrays.asList("p value2"));

        expectedResponseHeaders = new HashMap<>();
        expectedResponseHeaders.put("header1", "header value1");
        expectedResponseHeaders.put("header2", "header value2");

        expectedEntity = new Entity();

        expectedBody = "some text";

        prototypeUri = new URI("http://prototype.host/path/to/resource");

        server = new HttpServer(serverThreadCount, 0);
        server.start();
        serverPort = server.getPort();
    }

    @After
    public void teardown() {
        if (server.isRunning()) {
            server.stop();
        }
        if (replicator.isRunning()) {
            replicator.stop();
        }
    }

    @Test
    public void testReplicateGetLessNodesThanReplicatorThreads() throws Throwable {
        testReplicateXXX(executorThreadCount - 1, HttpMethod.GET);
    }

    @Test
    public void testReplicateGetMoreNodesThanReplicatorThreads() throws Throwable {
        testReplicateXXX(executorThreadCount + 1, HttpMethod.GET);
    }

    @Test
    public void testReplicateGetWithUnresponsiveNode() throws Throwable {

        // nodes
        Set<NodeIdentifier> nodeIds = createNodes(2, "localhost", serverPort);

        // response
        HttpResponse expectedResponse = new HttpResponse(Status.OK, expectedBody);

        // first response normal, second response slow
        server.addResponseAction(new HttpResponseAction(expectedResponse));
        server.addResponseAction(new HttpResponseAction(expectedResponse, 3500));

        Set<NodeResponse> responses = replicator.replicate(
                nodeIds,
                HttpMethod.GET,
                prototypeUri,
                expectedRequestParameters,
                expectedRequestHeaders);

        assertEquals(nodeIds.size(), responses.size());

        Iterator<NodeResponse> nodeResponseItr = responses.iterator();

        NodeResponse firstResponse = nodeResponseItr.next();
        NodeResponse secondResponse = nodeResponseItr.next();
        NodeResponse goodResponse;
        NodeResponse badResponse;
        if (firstResponse.hasThrowable()) {
            goodResponse = secondResponse;
            badResponse = firstResponse;
        } else {
            goodResponse = firstResponse;
            badResponse = secondResponse;
        }

        // good response
        // check status
        assertEquals(Status.OK.getStatusCode(), goodResponse.getStatus());

        // check entity stream
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ((StreamingOutput) goodResponse.getResponse().getEntity()).write(baos);
        assertEquals("some text", new String(baos.toByteArray()));

        // bad response
        assertTrue(badResponse.hasThrowable());
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), badResponse.getStatus());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicateGetWithEntity() throws Throwable {
        testReplicateXXXEntity(HttpMethod.GET);
    }

    @Test
    public void testReplicatePost() throws Throwable {
        testReplicateXXX(HttpMethod.POST);
    }

    @Test
    public void testReplicatePostWithEntity() throws Throwable {
        testReplicateXXXEntity(HttpMethod.POST);
    }

    @Test
    public void testReplicatePut() throws Throwable {
        testReplicateXXX(HttpMethod.PUT);
    }

    @Test
    public void testReplicatePutWithEntity() throws Throwable {
        testReplicateXXXEntity(HttpMethod.PUT);
    }

    @Test
    public void testReplicateDelete() throws Throwable {
        testReplicateXXX(HttpMethod.DELETE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicateDeleteWithEntity() throws Throwable {
        testReplicateXXXEntity(HttpMethod.DELETE);
    }

    @Test
    public void testReplicateHead() throws Throwable {
        testReplicateXXX(HttpMethod.HEAD);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicateHeadWithEntity() throws Throwable {
        testReplicateXXXEntity(HttpMethod.HEAD);
    }

    @Test
    public void testReplicateOptions() throws Throwable {
        testReplicateXXX(HttpMethod.OPTIONS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicateOptionsWithEntity() throws Throwable {
        testReplicateXXXEntity(HttpMethod.OPTIONS);
    }

    private void testReplicateXXX(final String method) throws Throwable {
        testReplicateXXX(executorThreadCount, method);
    }

    private void testReplicateXXX(final int numNodes, final String method) throws Throwable {

        // nodes
        Set<NodeIdentifier> nodeIds = createNodes(numNodes, "localhost", serverPort);

        // set up responses
        for (int i = 0; i < nodeIds.size(); i++) {
            HttpResponse response = new HttpResponse(Status.OK, expectedBody);
            response.addHeaders(expectedResponseHeaders);
            server.addResponseAction(new HttpResponseAction(response));
        }

        // setup request parameters
        server.addCheckedParameters(expectedRequestParameters);

        // request headers
        server.addCheckedHeaders(expectedRequestHeaders);

        Set<NodeResponse> responses = replicator.replicate(
                nodeIds,
                method,
                prototypeUri,
                expectedRequestParameters,
                expectedRequestHeaders);

        Set<NodeIdentifier> returnedNodeIds = new HashSet<>();
        for (NodeResponse response : responses) {

            // check if we received an exception
            if (response.hasThrowable()) {
                throw response.getThrowable();
            }

            // gather ids to verify later
            returnedNodeIds.add(response.getNodeId());

            // check status
            assertEquals(Status.OK.getStatusCode(), response.getStatus());

            Response serverResponse = response.getResponse();

            // check response headers are copied
            assertTrue(containsHeaders(expectedResponseHeaders, serverResponse.getMetadata()));

            // check entity stream
            if (HttpMethod.HEAD.equalsIgnoreCase(method)) {
                assertNull(serverResponse.getEntity());
            } else {
                assertTrue(isEquals((StreamingOutput) serverResponse.getEntity(), expectedBody));
            }

        }

        // check node Ids
        assertEquals(nodeIds, returnedNodeIds);
    }

    private void testReplicateXXXEntity(final String method) throws Throwable {
        testReplicateXXXEntity(executorThreadCount, method);
    }

    private void testReplicateXXXEntity(final int numNodes, final String method) throws Throwable {

        // nodes
        Set<NodeIdentifier> nodeIds = createNodes(numNodes, "localhost", serverPort);

        // set up responses
        for (int i = 0; i < nodeIds.size(); i++) {
            HttpResponse response = new HttpResponse(Status.OK, expectedBody);
            response.addHeaders(expectedResponseHeaders);
            server.addResponseAction(new HttpResponseAction(response));
        }

        // headers
        expectedRequestHeaders.put("Content-Type", "application/xml");

        Set<NodeResponse> responses = replicator.replicate(
                nodeIds,
                method,
                prototypeUri,
                expectedEntity,
                expectedRequestHeaders);

        Set<NodeIdentifier> returnedNodeIds = new HashSet<>();
        for (NodeResponse response : responses) {

            // check if we received an exception
            if (response.hasThrowable()) {
                throw response.getThrowable();
            }

            // gather ids to verify later
            returnedNodeIds.add(response.getNodeId());

            // check status
            assertEquals(Status.OK.getStatusCode(), response.getStatus());

            Response serverResponse = response.getResponse();

            // check response headers are copied
            assertTrue(containsHeaders(expectedResponseHeaders, serverResponse.getMetadata()));

            // check entity stream
            assertTrue(isEquals((StreamingOutput) serverResponse.getEntity(), expectedBody));

        }

        // check node Ids
        assertEquals(nodeIds, returnedNodeIds);
    }

    private Set<NodeIdentifier> createNodes(int num, String host, int apiPort) {
        Set<NodeIdentifier> result = new HashSet<>();
        for (int i = 0; i < num; i++) {
            result.add(new NodeIdentifier(String.valueOf(i), host, apiPort, host, 1, "localhost", 1234, false));
        }
        return result;
    }

    private boolean isEquals(StreamingOutput so, String expectedText) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        so.write(baos);
        return expectedText.equals(new String(baos.toByteArray()));
    }

    private boolean containsHeaders(Map<String, String> expectedHeaders, MultivaluedMap<String, Object> metadata) {
        for (Map.Entry<String, String> expectedEntry : expectedHeaders.entrySet()) {
            if (expectedEntry.getValue().equals(metadata.getFirst(expectedEntry.getKey())) == false) {
                return false;
            }
        }
        return true;
    }

}

@XmlRootElement
class Entity {
}
