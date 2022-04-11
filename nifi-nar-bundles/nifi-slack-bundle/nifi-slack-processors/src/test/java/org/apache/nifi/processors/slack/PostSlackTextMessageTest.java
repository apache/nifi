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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostSlackTextMessageTest {
    private static final String RESPONSE_SUCCESS_TEXT_MSG = "{\"ok\": true}";
    private static final String RESPONSE_SUCCESS_FILE_MSG = "{\"ok\": true, \"file\": {\"url_private\": \"slack-file-url\"}}";
    private static final String RESPONSE_WARNING = "{\"ok\": true, \"warning\": \"slack-warning\"}";
    private static final String RESPONSE_ERROR = "{\"ok\": false, \"error\": \"slack-error\"}";
    private static final String RESPONSE_EMPTY_JSON = "{}";
    private static final String RESPONSE_INVALID_JSON = "{invalid-json}";

    private TestRunner testRunner;

    private MockWebServer mockWebServer;

    private String url;

    @BeforeEach
    public void init() {
        mockWebServer = new MockWebServer();
        url = mockWebServer.url("/").toString();
        testRunner = TestRunners.newTestRunner(PostSlack.class);
    }

    @Test
    public void sendTextOnlyMessageSuccessfully() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_TEXT_MSG));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals("my-text", requestBodyJson.getString("text"));
    }

    @Test
    public void sendTextWithAttachmentMessageSuccessfully() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

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
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals("[{\"my-attachment-key\":\"my-attachment-value\"}]", requestBodyJson.getJsonArray("attachments").toString());
    }

    @Test
    public void processShouldFailWhenChannelIsEmpty() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "${dummy}");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void processShouldFailWhenTextIsEmptyAndNoAttachmentSpecified() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "${dummy}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void emptyAttachmentShouldBeSkipped() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "${dummy}");
        testRunner.setProperty("attachment_02", "{\"my-attachment-key\": \"my-attachment-value\"}");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

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
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty("attachment_01", "{invalid-json}");
        testRunner.setProperty("attachment_02", "{\"my-attachment-key\": \"my-attachment-value\"}");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

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
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void processShouldFailWhenSlackReturnsError() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_ERROR));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void processShouldNotFailWhenSlackReturnsWarning() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_WARNING));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        assertBasicRequest(getRequestBodyJson());
    }

    @Test
    public void processShouldFailWhenSlackReturnsEmptyJson() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_EMPTY_JSON));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void processShouldFailWhenSlackReturnsInvalidJson() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_INVALID_JSON));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void sendInternationalMessageSuccessfully() {
        testRunner.setProperty(PostSlack.POST_MESSAGE_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "Iñtërnâtiônàližætiøn");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_TEXT_MSG));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        JsonObject requestBodyJson = getRequestBodyJson();
        assertBasicRequest(requestBodyJson);
        assertEquals("Iñtërnâtiônàližætiøn", requestBodyJson.getString("text"));
    }

    private void assertBasicRequest(JsonObject requestBodyJson) {
        assertEquals("my-channel", requestBodyJson.getString("channel"));
    }

    private JsonObject getRequestBodyJson() {
        try {
            final RecordedRequest recordedRequest = mockWebServer.takeRequest();
            try (final JsonReader reader = Json.createReader(new InputStreamReader(
                    recordedRequest.getBody().inputStream(), StandardCharsets.UTF_8))) {
                return reader.readObject();
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
