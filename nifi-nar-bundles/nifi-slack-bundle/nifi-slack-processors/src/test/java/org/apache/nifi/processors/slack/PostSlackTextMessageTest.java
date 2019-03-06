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
package org.apache.nifi.processors.slack;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.TestServer;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;

import static org.apache.nifi.processors.slack.PostSlackCaptureServlet.REQUEST_PATH_EMPTY_JSON;
import static org.apache.nifi.processors.slack.PostSlackCaptureServlet.REQUEST_PATH_ERROR;
import static org.apache.nifi.processors.slack.PostSlackCaptureServlet.REQUEST_PATH_INVALID_JSON;
import static org.apache.nifi.processors.slack.PostSlackCaptureServlet.REQUEST_PATH_SUCCESS_TEXT_MSG;
import static org.apache.nifi.processors.slack.PostSlackCaptureServlet.REQUEST_PATH_WARNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PostSlackTextMessageTest {

    private TestRunner testRunner;

    private TestServer server;
    private PostSlackCaptureServlet servlet;

    @Before
    public void setup() throws Exception {
        testRunner = TestRunners.newTestRunner(PostSlack.class);

        servlet = new PostSlackCaptureServlet();

        ServletContextHandler handler = new ServletContextHandler();
        handler.addServlet(new ServletHolder(servlet), "/*");

        server = new TestServer();
        server.addHandler(handler);
        server.startServer();
    }

    @Test
    public void sendTextOnlyMessageSuccessfully() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_SUCCESS_TEXT_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals("my-text", requestBodyJson.getString("text"));
    }

    @Test
    public void sendTextWithAttachmentMessageSuccessfully() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_SUCCESS_TEXT_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals("my-text", requestBodyJson.getString("text"));
        assertEquals("[{\"my-attachment-key\":\"my-attachment-value\"}]", requestBodyJson.getJsonArray("attachments").toString());
    }

    @Test
    public void sendAttachmentOnlyMessageSuccessfully() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_SUCCESS_TEXT_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals("[{\"my-attachment-key\":\"my-attachment-value\"}]", requestBodyJson.getJsonArray("attachments").toString());
    }

    @Test
    public void processShouldFailWhenChannelIsEmpty() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl());
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "${dummy}");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);

        assertFalse(servlet.hasBeenInteracted());
    }

    @Test
    public void processShouldFailWhenTextIsEmptyAndNoAttachmentSpecified() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl());
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "${dummy}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);

        assertFalse(servlet.hasBeenInteracted());
    }

    @Test
    public void emptyAttachmentShouldBeSkipped() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_SUCCESS_TEXT_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "${dummy}");
        testRunner.setProperty("attachment_02", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals(1, requestBodyJson.getJsonArray("attachments").size());
        assertEquals("[{\"my-attachment-key\":\"my-attachment-value\"}]", requestBodyJson.getJsonArray("attachments").toString());
    }

    @Test
    public void invalidAttachmentShouldBeSkipped() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_SUCCESS_TEXT_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "{invalid-json}");
        testRunner.setProperty("attachment_02", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals(1, requestBodyJson.getJsonArray("attachments").size());
        assertEquals("[{\"my-attachment-key\":\"my-attachment-value\"}]", requestBodyJson.getJsonArray("attachments").toString());
    }

    @Test
    public void processShouldFailWhenHttpErrorCodeReturned() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl());
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void processShouldFailWhenSlackReturnsError() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_ERROR);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void processShouldNotFailWhenSlackReturnsWarning() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_WARNING);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        assertBasicRequest(getRequestBodyJson());
    }

    @Test
    public void processShouldFailWhenSlackReturnsEmptyJson() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_EMPTY_JSON);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void processShouldFailWhenSlackReturnsInvalidJson() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_INVALID_JSON);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void sendInternationalMessageSuccessfully() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, server.getUrl() + REQUEST_PATH_SUCCESS_TEXT_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "Iñtërnâtiônàližætiøn");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals("Iñtërnâtiônàližætiøn", requestBodyJson.getString("text"));
    }

    private void assertBasicRequest(JsonObject requestBodyJson) {
        Map<String, String> requestHeaders = servlet.getLastPostHeaders();
        assertEquals("Bearer my-access-token", requestHeaders.get("Authorization"));
        assertEquals("application/json; charset=UTF-8", requestHeaders.get("Content-Type"));

        assertEquals("my-channel", requestBodyJson.getString("channel"));
    }

    private JsonObject getRequestBodyJson() {
        return Json.createReader(
                new InputStreamReader(
                        new ByteArrayInputStream(servlet.getLastPostBody()), Charset.forName("UTF-8")))
                .readObject();
    }
}
