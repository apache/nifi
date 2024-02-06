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

package org.apache.nifi.processors.aws.wag;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.util.RegionUtilV1;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.proxy.StandardProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TestInvokeAWSGatewayApiCommon {

    private static final String SUCCESS_RESPONSE_BODY = "{\"status\":\"200\"}";

    private static final String APPLICATION_JSON = "application/json";

    public TestRunner runner;

    protected MockWebServer mockWebServer;

    protected void setupControllerService() {
        AuthUtils.enableAccessKey(runner, "awsAccessKey", "awsSecretKey");
    }

    protected void setupAuth() {
        AuthUtils.enableAccessKey(runner, "awsAccessKey", "awsSecretKey");
    }

    protected void setupCredFile() throws InitializationException {
        AuthUtils.enableCredentialsFile(runner, "src/test/resources/mock-aws-credentials.properties");
    }

    public void setupEndpointAndRegion() {
        runner.setProperty(RegionUtilV1.REGION, "us-east-1");
        runner.setProperty(InvokeAWSGatewayApi.PROP_AWS_API_KEY, "abcd");
        runner.setProperty(InvokeAWSGatewayApi.PROP_AWS_GATEWAY_API_ENDPOINT, mockWebServer.url("/").toString());
    }

    private void enqueueSuccess() {
        mockWebServer.enqueue(
                new MockResponse()
                        .setResponseCode(200)
                        .addHeader("Content-Type", APPLICATION_JSON)
                        .setBody(SUCCESS_RESPONSE_BODY)
        );
    }

    @Test
    public void test200() throws Exception {
        enqueueSuccess();
        enqueueSuccess();

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        createFlowFiles(runner);

        // verify that call works with or without flowfile being sent for GET
        // there should only be 1 REQ, but 2 RESPONSE
        runner.run();
        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 2);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response of each message
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        assert200Response(runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0), true);
        assert200Response(runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(1), false);

        final List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertEquals(3, provEvents.size());
        boolean forkEvent = false;
        boolean fetchEvent = false;
        boolean recieveEvent = false;
        for (final ProvenanceEventRecord event : provEvents) {
            if (event.getEventType() == ProvenanceEventType.FORK) {
                forkEvent = true;
            } else if (event.getEventType() == ProvenanceEventType.FETCH) {
                fetchEvent = true;
            } else if (event.getEventType() == ProvenanceEventType.RECEIVE) {
                recieveEvent = true;
            }
        }

        assertTrue(forkEvent);
        assertTrue(fetchEvent);
        assertTrue(recieveEvent);
    }

    private void assert200Response(final MockFlowFile bundle, final boolean requestWithInput) throws IOException {
        bundle.assertContentEquals("{\"status\":\"200\"}".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Content-Type", "application/json");

        // check any input FlowFile attributes were included in the Response FlowFile
        if (requestWithInput) {
            bundle.assertAttributeEquals("Foo", "Bar");
        } else {
            bundle.assertAttributeNotExists("Foo");
        }
    }

    @Test
    public void testOutputResponseRegardless() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(404));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_OUTPUT_RESPONSE_REGARDLESS, "true");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "404");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Not Found");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "404");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Not Found");
        bundle1.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testOutputResponseRegardlessWithOutputInAttribute() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(404));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_OUTPUT_RESPONSE_REGARDLESS, "true");
        runner.setProperty(InvokeAWSGatewayApi.PROP_PUT_OUTPUT_IN_ATTRIBUTE, "outputBody");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "404");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Not Found");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "404");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Not Found");
        bundle1.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testOutputResponseSetMimeTypeToResponseContentType() throws Exception {
        enqueueSuccess();

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        createFlowFiles(runner);
        runner.setProperty(InvokeAWSGatewayApi.PROP_OUTPUT_RESPONSE_REGARDLESS, "true");
        runner.setProperty(InvokeAWSGatewayApi.PROP_PUT_OUTPUT_IN_ATTRIBUTE, "outputBody");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "application/json");
        bundle1.assertAttributeEquals("mime.type", "application/json");
    }

    @Test
    public void testOutputResponseRegardlessWithOutputInAttributeLarge() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(404));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        createFlowFiles(runner);
        runner.setProperty(InvokeAWSGatewayApi.PROP_OUTPUT_RESPONSE_REGARDLESS, "true");
        runner.setProperty(InvokeAWSGatewayApi.PROP_PUT_OUTPUT_IN_ATTRIBUTE, "outputBody");
        runner.setProperty(InvokeAWSGatewayApi.PROP_PUT_ATTRIBUTE_MAX_LENGTH, "11");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "404");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Not Found");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "404");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Not Found");
        bundle1.assertAttributeEquals("Foo", "Bar");
    }


    @Test
    public void testMultipleSameHeaders() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("double", "2").addHeader("double", "2"));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        // amazon does not support headers with the same name, we'll only get 2 here
        // this is in the amazon layer, we only get Map<String,String> for headers so the 1 has been stripped
        // already
        bundle1.assertAttributeEquals("double", "2,2");
    }

    @Test
    public void testPutResponseHeadersInRequest() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", APPLICATION_JSON).addHeader("double", "2"));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_ADD_HEADERS_TO_REQUEST, "true");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+all attributes from response)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");
        bundle.assertAttributeEquals("double", "2");
        bundle.assertAttributeEquals("Content-Type", APPLICATION_JSON);

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("double", "2");
        bundle1.assertAttributeEquals("Content-Type", APPLICATION_JSON);
    }

    @Test
    public void testToRequestAttribute() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_PUT_OUTPUT_IN_ATTRIBUTE, "outputBody");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code, status.message and body of response in attribute
        // original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testNoInput() {
        enqueueSuccess();

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Content-Type", APPLICATION_JSON);
    }

    @Test
    public void testNoInputWithAttributes() {
        enqueueSuccess();

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_ATTRIBUTES_TO_SEND, "myAttribute");
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Content-Type", "application/json");
    }

    @Test
    public void testNoInputSendToAttribute() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_PUT_OUTPUT_IN_ATTRIBUTE, "outputBody");
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request
        // status code, status message, no ff content
        // server response message body into attribute of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ).get(0);
        bundle1.assertContentEquals("".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
    }

    @Test
    public void test500() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/500");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(1);

        // expected in response
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RETRY_NAME).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "500");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Internal Server Error");

        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test300() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(302));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/302");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "302");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Moved Temporarily");
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test304() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(304));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/304");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "304");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Not Modified");
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test400() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(400));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/400");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);
        // expected in response
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "400");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Bad Request");
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test400WithPenalizeNoRetry() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(400));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/400");
        runner.setProperty(InvokeAWSGatewayApi.PROP_PENALIZE_NO_RETRY, "true");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(1);
        // expected in response
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "400");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Bad Request");
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void test412() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(412));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/412");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_NO_RETRY_NAME).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "412");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "Precondition Failed");
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void testHead() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "HEAD");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertContentEquals("".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        assertEquals(expected1, actual1);
    }

    @Test
    public void testPost() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "POST");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertContentEquals("".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        assertEquals(expected1, actual1);
    }

    @Test
    public void testPostWithMimeType() {
        final String suppliedMimeType = "text/plain";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", suppliedMimeType));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "POST");

        final Map<String, String> attrs = new HashMap<>();

        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
    }

    @Test
    public void testPostWithEmptyELExpression() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", InvokeAWSGatewayApi.DEFAULT_CONTENT_TYPE));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "POST");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
    }

    @Test
    public void testPostWithContentTypeProperty() {
        final String suppliedMimeType = "text/plain";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", suppliedMimeType));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "POST");
        runner.setProperty(InvokeAWSGatewayApi.PROP_CONTENT_TYPE, suppliedMimeType);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
    }

    @Test
    public void testPostWithEmptyBodySet() {
        final String suppliedMimeType = "";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", suppliedMimeType));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "POST");
        runner.setProperty(InvokeAWSGatewayApi.PROP_CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(InvokeAWSGatewayApi.PROP_SEND_BODY, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
    }


    @Test
    public void testPutWithMimeType() {
        final String suppliedMimeType = "text/plain";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", suppliedMimeType));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PUT");

        final Map<String, String> attrs = new HashMap<>();

        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
    }

    @Test
    public void testPutWithEmptyELExpression() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", InvokeAWSGatewayApi.DEFAULT_CONTENT_TYPE));
        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PUT");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
    }

    @Test
    public void testPutWithContentTypeProperty() {
        final String suppliedMimeType = "text/plain";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", suppliedMimeType));
        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PUT");
        runner.setProperty(InvokeAWSGatewayApi.PROP_CONTENT_TYPE, suppliedMimeType);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
    }

    @Test
    public void testPut() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/post");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PUT");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        assertEquals(expected1, actual1);
    }

    @Test
    public void testPatch() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));
        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/patch");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PATCH");


        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        assertEquals(expected1, actual1);
    }

    @Test
    public void testPatchWithMimeType() {
        final String suppliedMimeType = "text/plain";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", suppliedMimeType));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/patch");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PATCH");

        final Map<String, String> attrs = new HashMap<>();

        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
    }

    @Test
    public void testPatchWithEmptyELExpression() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", InvokeAWSGatewayApi.DEFAULT_CONTENT_TYPE));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/patch");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PATCH");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
    }

    @Test
    public void testPatchWithContentTypeProperty() {
        final String suppliedMimeType = "text/plain";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", suppliedMimeType));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/patch");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "PATCH");
        runner.setProperty(InvokeAWSGatewayApi.PROP_CONTENT_TYPE, suppliedMimeType);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
    }

    @Test
    public void testDelete() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));
        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "DELETE");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes(StandardCharsets.UTF_8));
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        assertEquals(expected1, actual1);
    }

    @Test
    public void testOptions() throws Exception {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));
        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "OPTIONS");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testSendAttributes() throws Exception {
        enqueueSuccess();

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_ATTRIBUTES_TO_SEND, "F.*");
        final String dynamicValue = "testing";
        runner.setProperty("dynamicHeader", dynamicValue);

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 0);
        runner.assertPenalizeCount(0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE_NAME).get(0);
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", APPLICATION_JSON);

        final RecordedRequest recordedRequest = mockWebServer.takeRequest();
        assertEquals(dynamicValue, recordedRequest.getHeader("dynamicHeader"));
    }

    @Test
    public void testReadTimeout() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setHeadersDelay(1, TimeUnit.SECONDS));

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.TIMEOUT, "500 ms");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 1);
        runner.assertPenalizeCount(1);

        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_FAILURE_NAME).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testConnectFailBadPort() {
        setupEndpointAndRegion();
        // this is the bad urls
        final String badurlport = "http://localhost:" + 445;

        runner.setProperty(InvokeAWSGatewayApi.PROP_AWS_GATEWAY_API_ENDPOINT, badurlport);
        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/doesnotExist");
        runner.setProperty(InvokeAWSGatewayApi.TIMEOUT, "1 sec");
        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 1);
        runner.assertPenalizeCount(1);

        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_FAILURE_NAME).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testConnectFailBadHost() {
        setupEndpointAndRegion();
        final String badurlhost = "http://localhOOst:" + 445;

        runner.setProperty(InvokeAWSGatewayApi.PROP_AWS_GATEWAY_API_ENDPOINT, badurlhost);
        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/doesnotExist");
        runner.setProperty(InvokeAWSGatewayApi.TIMEOUT, "1 sec");
        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY_NAME, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE_NAME, 1);
        runner.assertPenalizeCount(1);

        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_FAILURE_NAME).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testArbitraryRequestFailsValidation() {

        setupEndpointAndRegion();

        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");
        runner.setProperty(InvokeAWSGatewayApi.PROP_METHOD, "FETCH");

        createFlowFiles(runner);

        assertThrows(AssertionError.class, runner::run);
    }

    @Test
    public void testProxy() throws Exception {
        final String contentType = "text/plain;charset=iso-8859-1";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", contentType));
        setupEndpointAndRegion();
        URL proxyURL = mockWebServer.url("/").url();

        runner.setEnvironmentVariableValue("proxy.host", proxyURL.getHost());
        runner.setEnvironmentVariableValue("proxy.port", String.valueOf(proxyURL.getPort()));
        runner.setEnvironmentVariableValue("proxy.username", "username");
        runner.setEnvironmentVariableValue("proxy.password", "password");

        runner.setProperty(InvokeAWSGatewayApi.PROP_AWS_GATEWAY_API_ENDPOINT, "http://nifi.apache.org/");
        runner.setProperty(InvokeAWSGatewayApi.PROP_RESOURCE_NAME, "/status/200");

        final StandardProxyConfigurationService proxyService = new StandardProxyConfigurationService();
        runner.addControllerService("proxy", proxyService);
        runner.setProperty(proxyService, StandardProxyConfigurationService.PROXY_TYPE, Proxy.Type.HTTP.name());
        runner.setProperty(proxyService, StandardProxyConfigurationService.PROXY_SERVER_HOST, "${proxy.host}");
        runner.setProperty(proxyService, StandardProxyConfigurationService.PROXY_SERVER_PORT, "${proxy.port}");
        runner.setProperty(proxyService, StandardProxyConfigurationService.PROXY_USER_NAME, "${proxy.username}");
        runner.setProperty(proxyService, StandardProxyConfigurationService.PROXY_USER_PASSWORD, "${proxy.password}");
        runner.enableControllerService(proxyService);
        runner.setProperty(InvokeAWSGatewayApi.PROXY_CONFIGURATION_SERVICE, "proxy");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeAWSGatewayApi.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeAWSGatewayApi.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner
                .getFlowFilesForRelationship(InvokeAWSGatewayApi.REL_RESPONSE).get(0);

        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeAWSGatewayApi.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", contentType);
    }

    public static void createFlowFiles(final TestRunner testRunner) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
        attributes.put("Foo", "Bar");
        testRunner.enqueue("Hello".getBytes(StandardCharsets.UTF_8), attributes);
    }
}
