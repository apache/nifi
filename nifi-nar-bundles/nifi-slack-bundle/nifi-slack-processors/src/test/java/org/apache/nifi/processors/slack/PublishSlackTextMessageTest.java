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

import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.model.Attachment;
import org.apache.nifi.processors.slack.publish.PublishSlackClient;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.nifi.processors.slack.MockPublishSlackClient.EMPTY_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.ERROR_CHANNEL;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.ERROR_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.FLOWFILE_CONTENT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.INTERNAL_ERROR_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.INTERNATIONAL_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.INVALID_CHANNEL;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.INVALID_TOKEN;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.NOT_IN_CHANNEL_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.TOKEN_REVOKED_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.VALID_CHANNEL;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.VALID_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.VALID_TOKEN;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.WARNING_CHANNEL;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.WARNING_TEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublishSlackTextMessageTest {

    private TestRunner testRunner;

    private MockPublishSlackClient mockClient;

    @BeforeEach
    public void init() {
        mockClient = new MockPublishSlackClient();
        // Create an instance of the processor that mocks out the initialize() method to return a client we can use for testing
        final PublishSlack processor = new PublishSlack() {
            @Override
            protected PublishSlackClient initializeClient(final MethodsClient client) {
                return mockClient;
            }
        };
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void sendTextOnlyMessageSuccessfully() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, VALID_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertEquals(VALID_TEXT, request.getText());
        assertNull(request.getAttachments());
    }

    @Test
    public void sendFlowFileContentOnlyMessageSuccessfully() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_CONTENT_MESSAGE);

        testRunner.enqueue(FLOWFILE_CONTENT);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertEquals(FLOWFILE_CONTENT, request.getText());
        assertNull(request.getAttachments());
    }

    @Test
    public void sendTextWithAttachmentMessageSuccessfully() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, VALID_TEXT);
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertEquals(VALID_TEXT, request.getText());
        List<Attachment> attachments = request.getAttachments();
        assertNotNull(attachments);
        assertEquals(1, attachments.size());
        assertEquals("{\"my-attachment-key\":\"my-attachment-value\"}", attachments.get(0).getText());
    }

    @Test
    public void sendAttachmentOnlyMessageSuccessfully() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertNull(request.getText());
        List<Attachment> attachments = request.getAttachments();
        assertNotNull(attachments);
        assertEquals(1, attachments.size());
        assertEquals("{\"my-attachment-key\":\"my-attachment-value\"}", attachments.get(0).getText());
    }

    @Test
    public void processShouldFailWhenInvalidToken() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, INVALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, VALID_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_FAILURE);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(INVALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertEquals(VALID_TEXT, request.getText());
        ChatPostMessageResponse response = mockClient.getLastChatPostMessageResponse();
        assertFalse(response.isOk());
        assertEquals(TOKEN_REVOKED_TEXT, response.getError());
    }

    @Test
    public void processShouldFailWhenInvalidChannel() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, INVALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, VALID_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_FAILURE);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(INVALID_CHANNEL, request.getChannel());
        assertEquals(VALID_TEXT, request.getText());
        ChatPostMessageResponse response = mockClient.getLastChatPostMessageResponse();
        assertFalse(response.isOk());
        assertEquals(NOT_IN_CHANNEL_TEXT, response.getError());
    }

    @Test
    public void processShouldFailWhenChannelIsEmpty() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, EMPTY_TEXT);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, VALID_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_FAILURE);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertNull(request);
    }

    @Test
    public void processShouldFailWhenTextIsEmptyAndNoAttachmentSpecified() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, EMPTY_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_FAILURE);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertNull(request);
    }

    @Test
    public void emptyAttachmentShouldBeSkipped() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty("attachment_01", EMPTY_TEXT);
        testRunner.setProperty("attachment_02", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertNull(request.getText());
        List<Attachment> attachments = request.getAttachments();
        assertNotNull(attachments);
        assertEquals(1, attachments.size());
        assertEquals("{\"my-attachment-key\":\"my-attachment-value\"}", attachments.get(0).getText());
    }

    @Test
    public void invalidAttachmentShouldBeSkipped() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty("attachment_01", "{invalid-json}");
        testRunner.setProperty("attachment_02", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertNull(request.getText());
        List<Attachment> attachments = request.getAttachments();
        assertNotNull(attachments);
        assertEquals(1, attachments.size());
        assertEquals("{\"my-attachment-key\":\"my-attachment-value\"}", attachments.get(0).getText());
    }

    @Test
    public void processShouldFailWhenHttpErrorCodeReturned() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, ERROR_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, INTERNAL_ERROR_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_FAILURE);

        List<LogMessage> logMessages = testRunner.getLogger().getErrorMessages();
        assertEquals(1, logMessages.size());
        assertNotNull(logMessages.get(0).getMsg());
        assertTrue(logMessages.get(0).getMsg().contains("Failed to send message to Slack."));
        logMessages = testRunner.getLogger().getDebugMessages();
        assertEquals(1, logMessages.size());
        assertNotNull(logMessages.get(0).getMsg());
        assertTrue(logMessages.get(0).getMsg().contains(INTERNAL_ERROR_TEXT));

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(ERROR_CHANNEL, request.getChannel());
        assertEquals(INTERNAL_ERROR_TEXT, request.getText());
        ChatPostMessageResponse response = mockClient.getLastChatPostMessageResponse();
        assertFalse(response.isOk());
        assertEquals(INTERNAL_ERROR_TEXT, response.getError());
    }

    @Test
    public void processShouldFailWhenSlackReturnsError() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, ERROR_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, ERROR_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_FAILURE);

        List<LogMessage> logMessages = testRunner.getLogger().getErrorMessages();
        assertEquals(1, logMessages.size());
        assertNotNull(logMessages.get(0).getMsg());
        assertTrue(logMessages.get(0).getMsg().contains("Failed to send message to Slack."));
        logMessages = testRunner.getLogger().getDebugMessages();
        assertEquals(1, logMessages.size());
        assertNotNull(logMessages.get(0).getMsg());
        assertTrue(logMessages.get(0).getMsg().contains(ERROR_TEXT));

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(ERROR_CHANNEL, request.getChannel());
        assertEquals(ERROR_TEXT, request.getText());
        ChatPostMessageResponse response = mockClient.getLastChatPostMessageResponse();
        assertFalse(response.isOk());
        assertEquals(ERROR_TEXT, response.getError());
    }

    @Test
    public void processShouldLogWarningAndNotFailWhenSlackReturnsWarning() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, WARNING_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, WARNING_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        List<LogMessage> warnings = testRunner.getLogger().getWarnMessages();
        assertEquals(1, warnings.size());
        assertNotNull(warnings.get(0).getMsg());
        assertTrue(warnings.get(0).getMsg().contains(WARNING_TEXT));

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(WARNING_CHANNEL, request.getChannel());
        assertEquals(WARNING_TEXT, request.getText());
        ChatPostMessageResponse response = mockClient.getLastChatPostMessageResponse();
        assertTrue(response.isOk());
        assertEquals(WARNING_TEXT, response.getWarning());
    }

    @Test
    public void sendInternationalMessageSuccessfully() {
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_MESSAGE_ONLY);
        testRunner.setProperty(PublishSlack.TEXT, INTERNATIONAL_TEXT);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        ChatPostMessageRequest request = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, request.getToken());
        assertEquals(VALID_CHANNEL, request.getChannel());
        assertEquals(INTERNATIONAL_TEXT, request.getText());
    }
}
