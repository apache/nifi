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

package org.apache.nifi.processors.standard.util;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.standard.InvokeHTTP;
import org.apache.nifi.processors.standard.TestServer;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.commons.codec.binary.Base64.encodeBase64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TestInvokeHttpCommon {

    public static TestServer server;
    public static String url;

    public TestRunner runner;


    public void addHandler(Handler handler) {
        server.addHandler(handler);
    }

    @Test
    public void testDateGeneration() throws Exception {

        final DateHandler dh = new DateHandler();
        addHandler(dh);

        runner.setProperty(InvokeHTTP.PROP_URL, url);
        createFlowFiles(runner);
        runner.run();

        // extract the date string sent to the server
        // and store it as a java.util.Date
        final SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        final Date date = sdf.parse(dh.dateString);

        // calculate the difference between the date string sent by the client and
        // the current system time -- these should be within a second or two
        // (just enough time to run the test).
        //
        // If the difference is more like in hours, it's likely that a timezone
        // conversion caused a problem.
        final long diff = Math.abs(System.currentTimeMillis() - date.getTime());
        final long threshold = 15000; // 15 seconds
        if (diff > threshold) {
            fail("Difference (" + diff + ") was greater than threshold (" + threshold + ")");
        }
        System.out.println("diff: " + diff);
    }

    @Test
    public void test200() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        // Verify only one FlowFile gets created/sent
        runner.run();
        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");

        final List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertEquals(2, provEvents.size());
        boolean forkEvent = false;
        boolean fetchEvent = false;
        for (final ProvenanceEventRecord event : provEvents) {
            if (event.getEventType() == ProvenanceEventType.FORK) {
                forkEvent = true;
            } else if (event.getEventType() == ProvenanceEventType.FETCH) {
                fetchEvent = true;
            }
        }

        assertTrue(forkEvent);
        assertTrue(fetchEvent);
    }

    @Test
    public void testOutputResponseRegardless() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_OUTPUT_RESPONSE_REGARDLESS,"true");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "404");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Not Found");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("NO".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "404");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Not Found");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testOutputResponseRegardlessWithOutputInAttribute() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_OUTPUT_RESPONSE_REGARDLESS,"true");
        runner.setProperty(InvokeHTTP.PROP_PUT_OUTPUT_IN_ATTRIBUTE,"outputBody");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "404");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Not Found");
        bundle.assertAttributeEquals("outputBody", "NO");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("NO".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "404");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Not Found");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testOutputResponseSetMimeTypeToResponseContentType() throws Exception {
        addHandler(new GetOrHeadHandler());

        String statusUrl = "/status/200";
        runner.setProperty(InvokeHTTP.PROP_URL, url + statusUrl);
        runner.setProperty(InvokeHTTP.PROP_METHOD, "GET");
        runner.setProperty(InvokeHTTP.PROP_OUTPUT_RESPONSE_REGARDLESS,"true");
        runner.setProperty(InvokeHTTP.PROP_PUT_OUTPUT_IN_ATTRIBUTE,"outputBody");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY,0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("outputBody", statusUrl);
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals(statusUrl.getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
        bundle1.assertAttributeEquals("mime.type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testOutputResponseRegardlessWithOutputInAttributeLarge() throws Exception {
        addHandler(new GetLargeHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_OUTPUT_RESPONSE_REGARDLESS,"true");
        runner.setProperty(InvokeHTTP.PROP_PUT_OUTPUT_IN_ATTRIBUTE,"outputBody");
        runner.setProperty(InvokeHTTP.PROP_PUT_ATTRIBUTE_MAX_LENGTH,"11");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "404");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Not Found");
        bundle.assertAttributeEquals("outputBody", "Lorem ipsum");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
                + "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor "
                + "in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, "
                + "sunt in culpa qui officia deserunt mollit anim id est laborum.");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "404");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Not Found");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }


    @Test
    public void testMultipleSameHeaders() throws Exception {
        addHandler(new GetMultipleHeaderHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("double", "1, 2");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testPutResponseHeadersInRequest() throws Exception {
        addHandler(new GetMultipleHeaderHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_ADD_HEADERS_TO_REQUEST, "true");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+all attributes from response)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");
        bundle.assertAttributeEquals("double", "1, 2");
        bundle.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("double", "1, 2");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testToRequestAttribute() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_PUT_OUTPUT_IN_ATTRIBUTE,"outputBody");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code, status.message and body of response in attribute
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals("outputBody", "/status/200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testNoInput() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD,"GET");
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testNoInputWithAttributes() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD, "GET");
        runner.setProperty(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND, "myAttribute");
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testNoInputFail() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        runner.setProperty(InvokeHTTP.PROP_METHOD,"OPTION");

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);
    }

    @Test
    public void testNoInputSendToAttribute() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_PUT_OUTPUT_IN_ATTRIBUTE, "outputBody");
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request
        // status code, status message, no ff content
        // server response message body into attribute of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals("outputBody", "/status/200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
    }

    @Test
    public void test200Auth() throws Exception {
        addHandler(new BasicAuthHandler());

        final String username = "basic_user";
        final String password = "basic_password";

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_USERNAME, username);
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_PASSWORD, password);
        final byte[] creds = String.format("%s:%s", username, password).getBytes(StandardCharsets.UTF_8);
        final String expAuth = String.format("Basic %s", new String(encodeBase64(creds)));

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        // expected in response
        // status code, status message, all headers from server response --> ff attributes
        // server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        final String bundle1Content = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(bundle1Content.startsWith(expAuth)); // use startsWith instead of equals so we can ignore line endings
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");

        final List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertEquals(2, provEvents.size());
        boolean forkEvent = false;
        boolean fetchEvent = false;
        for (final ProvenanceEventRecord event : provEvents) {
            if (event.getEventType() == ProvenanceEventType.FORK) {
                forkEvent = true;
            } else if (event.getEventType() == ProvenanceEventType.FETCH) {
                fetchEvent = true;
            }
        }

        assertTrue(forkEvent);
        assertTrue(fetchEvent);
    }

    @Test
    public void test401NotAuth() throws Exception {
        addHandler(new BasicAuthHandler());

        final String username = "basic_user";
        final String password = "basic_password";

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/401");
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_USERNAME, username);
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_PASSWORD, password);

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in request status.code and status.message
        // original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "401");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Unauthorized");
        bundle.assertAttributeEquals("Foo", "Bar");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);

        final String response = bundle.getAttribute(InvokeHTTP.RESPONSE_BODY);
        assertEquals(response, "Get off my lawn!"+System.lineSeparator());
    }

    @Test
    public void test200DigestAuth() throws Exception {
        addHandler(new DigestAuthHandler());
        final String username = "basic_user";
        final String password = "basic_password";

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_USERNAME, username);
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_PASSWORD, password);
        runner.setProperty(InvokeHTTP.PROP_DIGEST_AUTH,"true");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals(("DIGEST"+System.lineSeparator()).getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void test401DigestNotAuth() throws Exception {
        addHandler(new DigestAuthHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_DIGEST_AUTH,"false");
        runner.setProperty(InvokeHTTP.PROP_PUT_ATTRIBUTE_MAX_LENGTH,"512");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "401");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Unauthorized");
        bundle.assertAttributeEquals("Foo", "Bar");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void test500() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/500");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(1);

        // expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "500");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Server Error");
        bundle.assertAttributeEquals(InvokeHTTP.RESPONSE_BODY, "/status/500");

        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test300() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/302");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);
        // expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "302");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Found");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test304() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/304");

        createFlowFiles(runner);

        //assertTrue(server.jetty.isRunning());
        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);
        // expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "304");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Not Modified");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test400() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/400");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);
        // expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "400");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Bad Request");
        bundle.assertAttributeEquals(InvokeHTTP.RESPONSE_BODY, "/status/400");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void test400WithPenalizeNoRetry() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/400");
        runner.setProperty(InvokeHTTP.PROP_PENALIZE_NO_RETRY, "true");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(1);
        // expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "400");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Bad Request");
        bundle.assertAttributeEquals(InvokeHTTP.RESPONSE_BODY, "/status/400");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void test412() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/412");
        runner.setProperty(InvokeHTTP.PROP_METHOD, "GET");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 1);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        // expected in response
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_NO_RETRY).get(0);
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);

        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "412");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "Precondition Failed");
        bundle.assertAttributeEquals(InvokeHTTP.RESPONSE_BODY, "/status/412");
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

    }

    @Test
    public void testHead() throws Exception {
        addHandler(new GetOrHeadHandler());

        runner.setProperty(InvokeHTTP.PROP_METHOD, "HEAD");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testPost() throws Exception {
        addHandler(new MutativeMethodHandler(MutativeMethod.POST));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testPostWithMimeType() throws Exception {
        final String suppliedMimeType = "text/plain";
        addHandler(new MutativeMethodHandler(MutativeMethod.POST, suppliedMimeType));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");

        final Map<String, String> attrs = new HashMap<>();

        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPostWithEmptyELExpression() throws Exception {
        addHandler(new MutativeMethodHandler(MutativeMethod.POST, InvokeHTTP.DEFAULT_CONTENT_TYPE));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPostWithContentTypeProperty() throws Exception {
        final String suppliedMimeType = "text/plain";
        addHandler(new MutativeMethodHandler(MutativeMethod.POST, suppliedMimeType));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");
        runner.setProperty(InvokeHTTP.PROP_CONTENT_TYPE, suppliedMimeType);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPostWithEmptyBodySet() throws Exception {
        final String suppliedMimeType = "";
        addHandler(new MutativeMethodHandler(MutativeMethod.POST, suppliedMimeType));

        runner.setNonLoopConnection(false);
        runner.setProperty(InvokeHTTP.PROP_METHOD, "POST");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");
        runner.setProperty(InvokeHTTP.PROP_CONTENT_TYPE, suppliedMimeType);
        runner.setProperty(InvokeHTTP.PROP_SEND_BODY, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPutWithMimeType() throws Exception {
        final String suppliedMimeType = "text/plain";
        addHandler(new MutativeMethodHandler(MutativeMethod.PUT, suppliedMimeType));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PUT");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");

        final Map<String, String> attrs = new HashMap<>();

        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPutWithEmptyELExpression() throws Exception {
        addHandler(new MutativeMethodHandler(MutativeMethod.PUT, InvokeHTTP.DEFAULT_CONTENT_TYPE));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PUT");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPutWithContentTypeProperty() throws Exception {
        final String suppliedMimeType = "text/plain";
        addHandler(new MutativeMethodHandler(MutativeMethod.PUT, suppliedMimeType));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PUT");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");
        runner.setProperty(InvokeHTTP.PROP_CONTENT_TYPE, suppliedMimeType);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPut() throws Exception {
        addHandler(new MutativeMethodHandler(MutativeMethod.PUT));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PUT");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/post");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testPatch() throws Exception {
        addHandler(new MutativeMethodHandler(MutativeMethod.PATCH));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PATCH");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/patch");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeNotExists("Content-Type");

        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testPatchWithMimeType() throws Exception {
        final String suppliedMimeType = "text/plain";
        addHandler(new MutativeMethodHandler(MutativeMethod.PATCH, suppliedMimeType));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PATCH");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/patch");

        final Map<String, String> attrs = new HashMap<>();

        attrs.put(CoreAttributes.MIME_TYPE.key(), suppliedMimeType);
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPatchWithEmptyELExpression() throws Exception {
        addHandler(new MutativeMethodHandler(MutativeMethod.PATCH, InvokeHTTP.DEFAULT_CONTENT_TYPE));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PATCH");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/patch");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testPatchWithContentTypeProperty() throws Exception {
        final String suppliedMimeType = "text/plain";
        addHandler(new MutativeMethodHandler(MutativeMethod.PATCH, suppliedMimeType));

        runner.setProperty(InvokeHTTP.PROP_METHOD, "PATCH");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/patch");
        runner.setProperty(InvokeHTTP.PROP_CONTENT_TYPE, suppliedMimeType);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("Hello".getBytes(), attrs);

        runner.run(1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
    }

    @Test
    public void testDelete() throws Exception {
        addHandler(new DeleteHandler());

        runner.setProperty(InvokeHTTP.PROP_METHOD, "DELETE");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testOptions() throws Exception {
        addHandler(new OptionsHandler());

        runner.setProperty(InvokeHTTP.PROP_METHOD, "OPTIONS");
        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testSendAttributes() throws Exception {
        addHandler(new AttributesSentHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND, "F.*");
        runner.setProperty("dynamicHeader","yes!");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertContentEquals("Hello".getBytes("UTF-8"));
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("Bar".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("dynamicHeader","yes!");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
    }

    @Test
    public void testReadTimeout() throws Exception {
        addHandler(new ReadTimeoutHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_READ_TIMEOUT, "5 secs");

        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_FAILURE).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testConnectFailBadPort() throws Exception {
        addHandler(new GetOrHeadHandler());

        // this is the bad urls
        final String badurlport = "http://localhost:" + 445;

        runner.setProperty(InvokeHTTP.PROP_URL, badurlport + "/doesnotExist");
        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_FAILURE).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testConnectFailBadHost() throws Exception {
        addHandler(new GetOrHeadHandler());

        final String badurlhost = "http://localhOOst:" + 445;

        runner.setProperty(InvokeHTTP.PROP_URL, badurlhost + "/doesnotExist");
        createFlowFiles(runner);

        runner.run();
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_FAILURE).get(0);

        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");
    }

    @Test
    public void testArbitraryRequest() throws Exception {
        addHandler(new FetchHandler());

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD,"FETCH");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertPenalizeCount(0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertContentEquals("/status/200".getBytes("UTF-8"));
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");
        bundle1.assertAttributeEquals("Content-Type", "text/plain;charset=iso-8859-1");
        final String actual1 = new String(bundle1.toByteArray(), StandardCharsets.UTF_8);
        final String expected1 = "/status/200";
        Assert.assertEquals(expected1, actual1);
    }

    @Test
    public void testChunkedRequest() throws Exception {
        MutativeMethodHandler mutativeMethodHandler = new MutativeMethodHandler(MutativeMethod.POST);
        mutativeMethodHandler.setHeaderToTrack("Transfer-encoding");
        addHandler(mutativeMethodHandler);

        runner.setProperty(InvokeHTTP.PROP_URL, url + "/status/200");
        runner.setProperty(InvokeHTTP.PROP_METHOD,"POST");
        runner.setProperty(InvokeHTTP.PROP_USE_CHUNKED_ENCODING,"true");

        createFlowFiles(runner);

        runner.run();

        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);

        //expected in request status.code and status.message
        //original flow file (+attributes)
        final MockFlowFile bundle = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).get(0);
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        final String actual = new String(bundle.toByteArray(), StandardCharsets.UTF_8);
        final String expected = "Hello";
        Assert.assertEquals(expected, actual);
        bundle.assertAttributeEquals("Foo", "Bar");

        //expected in response
        //status code, status message, all headers from server response --> ff attributes
        //server response message body into payload of ff
        final MockFlowFile bundle1 = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).get(0);
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_CODE, "200");
        bundle1.assertAttributeEquals(InvokeHTTP.STATUS_MESSAGE, "OK");
        bundle1.assertAttributeEquals("Foo", "Bar");

        String header = mutativeMethodHandler.getTrackedHeaderValue();
        Assert.assertEquals("chunked",header);
    }


    public static void createFlowFiles(final TestRunner testRunner) throws UnsupportedEncodingException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
        attributes.put("Foo", "Bar");
        testRunner.enqueue("Hello".getBytes("UTF-8"), attributes);

    }


    protected static class DateHandler extends AbstractHandler {

        private String dateString;

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            dateString = request.getHeader("Date");

            response.setStatus(200);
            response.setContentType("text/plain");
            response.getWriter().println("Way to go!");
        }
    }

    private enum MutativeMethod { POST, PUT, PATCH }


    public static class MutativeMethodHandler extends AbstractHandler {
        private final MutativeMethod method;
        private final String expectedContentType;
        private String headerToTrack;
        private String trackedHeaderValue;

        public MutativeMethodHandler(final MutativeMethod method) {
            this(method, "application/plain-text");
        }

        public MutativeMethodHandler(final MutativeMethod method, final String expectedContentType) {
            this.method = method;
            this.expectedContentType = expectedContentType;
        }
        private void setHeaderToTrack(String headerToTrack){
            this.headerToTrack = headerToTrack;
        }

        public String getTrackedHeaderValue(){
            return trackedHeaderValue;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

            baseRequest.setHandled(true);

            if(method.name().equals(request.getMethod())) {
                if(this.expectedContentType.isEmpty()) {
                    Assert.assertNull(request.getHeader("Content-Type"));
                } else {
                    assertEquals(this.expectedContentType,request.getHeader("Content-Type"));
                }

                final String body = request.getReader().readLine();
                this.trackedHeaderValue = baseRequest.getHttpFields().get(headerToTrack);

                if(this.expectedContentType.isEmpty()) {
                    Assert.assertNull(body);
                } else {
                    assertEquals("Hello", body);
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }

        }

    }

    public static class GetOrHeadHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            final int status = Integer.valueOf(target.substring("/status".length() + 1));
            response.setStatus(status);

            if ("GET".equalsIgnoreCase(request.getMethod())) {
                if (status == 304){
                    // Status code 304 ("Not Modified") must not contain a message body
                    return;
                }

                response.setContentType("text/plain");
                response.setContentLength(target.length());

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(target);
                    writer.flush();
                }
            } else if(!"HEAD".equalsIgnoreCase(request.getMethod())) {
                response.setStatus(404);
                response.setContentType("text/plain");
                String body = "NO";
                response.setContentLength(body.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(body);
                    writer.flush();
                }
            }

        }
    }

    public static class GetLargeHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            final int status = Integer.valueOf(target.substring("/status".length() + 1));
            response.setStatus(status);

            response.setContentType("text/plain");
            response.setContentLength(target.length());

            if ("GET".equalsIgnoreCase(request.getMethod())) {
                try (PrintWriter writer = response.getWriter()) {
                    writer.print(target);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");

                //Lorem Ipsum
                String body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
                        + "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor "
                        + "in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, "
                        + "sunt in culpa qui officia deserunt mollit anim id est laborum.";

                response.setContentLength(body.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(body);
                    writer.flush();
                }
            }

        }
    }

    public static class GetMultipleHeaderHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            final int status = Integer.valueOf(target.substring("/status".length() + 1));
            response.setStatus(status);

            response.setContentType("text/plain");
            response.setContentLength(target.length());

            if ("GET".equalsIgnoreCase(request.getMethod())) {
                response.addHeader("double", "1");
                response.addHeader("double", "2");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(target);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }

        }
    }

    public static class DeleteHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            if ("DELETE".equalsIgnoreCase(request.getMethod())) {
                final int status = Integer.valueOf(target.substring("/status".length() + 1));
                response.setStatus(status);
                response.setContentLength(0);
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }
        }
    }

    public static class OptionsHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
                final int status = Integer.valueOf(target.substring("/status".length() + 1));
                response.setStatus(status);
                response.setContentLength(target.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(target);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(target.length());
            }
        }
    }

    public static class AttributesSentHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            if ("Get".equalsIgnoreCase(request.getMethod())) {
                String headerValue = request.getHeader("Foo");
                response.setHeader("dynamicHeader",request.getHeader("dynamicHeader"));
                final int status = Integer.valueOf(target.substring("/status".length() + 1));
                response.setStatus(status);
                response.setContentLength(headerValue.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(headerValue);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }
        }
    }

    public static class ReadTimeoutHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            if ("Get".equalsIgnoreCase(request.getMethod())) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    return;
                }
                String headerValue = request.getHeader("Foo");
                headerValue = headerValue == null ? "" : headerValue;
                final int status = Integer.valueOf(target.substring("/status".length() + 1));
                response.setStatus(status);
                response.setContentLength(headerValue.length());
                response.setContentType("text/plain");

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(headerValue);
                    writer.flush();
                }
            } else {
                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(0);
            }
        }
    }

    public static class BasicAuthHandler extends AbstractHandler {

        private String authString;

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            authString = request.getHeader("Authorization");

            if (authString == null) {
                response.setStatus(401);
                response.setHeader("WWW-Authenticate", "Basic realm=\"Jetty\"");
                response.setHeader("response.phrase", "Unauthorized");
                response.setContentType("text/plain");
                response.getWriter().println("Get off my lawn!");
                return;
            }

            final int status = Integer.valueOf(target.substring("/status".length() + 1));

            if (status == 200) {
                response.setStatus(status);
                response.setContentType("text/plain");
                response.getWriter().println(authString);
            } else {
                response.setStatus(status);
                response.setContentType("text/plain");
                response.getWriter().println("Get off my lawn!");
            }
        }
    }

    public static class DigestAuthHandler extends AbstractHandler {

        private DigestAuthenticator digestAuthenticator;

        private DigestAuthHandler() throws Exception {
            digestAuthenticator = new DigestAuthenticator();
            ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

            final HashLoginService hashLoginService = new HashLoginService("realm", "src/test/resources/TestInvokeHttp/realm.properties");
            hashLoginService.start();

            securityHandler.setLoginService(hashLoginService);
            securityHandler.setIdentityService(new DefaultIdentityService());
            digestAuthenticator.setConfiguration(securityHandler);
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse
                response)throws IOException, ServletException {
            baseRequest.setHandled(true);

            try {
                Authentication authentication = digestAuthenticator.validateRequest(request, response, true);

                if (authentication instanceof Authentication.User) {
                    response.setContentType("text/plain");
                    Authentication.User user = (Authentication.User) authentication;
                    response.getWriter().println(user.getAuthMethod());
                } else if (authentication instanceof Authentication.ResponseSent) {
                    Authentication.ResponseSent responseSent = (Authentication.ResponseSent) authentication;
                }
            } catch (ServerAuthException e) {
                e.printStackTrace();
            }
        }
    }

    public static class FetchHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);


            if ("Fetch".equalsIgnoreCase(request.getMethod())) {
                final int status = Integer.valueOf(target.substring("/status".length() + 1));
                response.setStatus(status);
                response.setContentType("text/plain");
                response.setContentLength(target.length());

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(target);
                    writer.flush();
                }
            } else {

                response.setStatus(404);
                response.setContentType("text/plain");
                response.setContentLength(target.length());
            }
        }
    }
}
