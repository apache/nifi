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
import com.slack.api.methods.request.files.FilesUploadV2Request;
import com.slack.api.model.Attachment;
import com.slack.api.model.File;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.slack.publish.PublishSlackClient;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.slack.MockPublishSlackClient.BASE_PERMA_LINK;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.BASE_URL_PRIVATE;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.EMPTY_TEXT;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.VALID_CHANNEL;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.VALID_CHANNEL_ID;
import static org.apache.nifi.processors.slack.MockPublishSlackClient.VALID_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublishSlackFileMessageTest {

    public static final String THREAD_TS = "some-thread-ts";
    public static final String TEXT = "my-text";
    public static final String INITIAL_COMMENT = "my-initial-comment";
    public static final String FILE_NAME = "my-file-name";
    public static final String FILE_TITLE = "my-file-title";
    public static final String FILE_DATA = "my-data";

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
    public void sendMessageWithBasicPropertiesSuccessfully() {
        // default strategy is STRATEGY_UPLOAD_ATTACH, so we don't need to set it
        // testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ATTACH);
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);

        Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), FILE_NAME);

        // in order not to make the assertion logic (even more) complicated, the file content is tested with character data instead of binary data
        testRunner.enqueue(FILE_DATA, flowFileAttributes);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        FilesUploadV2Request uploadRequest = mockClient.getLastFilesUploadRequest();
        assertEquals(VALID_TOKEN, uploadRequest.getToken());
        assertNull(uploadRequest.getChannel());
        assertNull(uploadRequest.getThreadTs());
        assertNull(uploadRequest.getInitialComment());
        assertEquals(FILE_NAME, uploadRequest.getFilename());
        assertEquals(FILE_NAME, uploadRequest.getTitle());
        assertEquals(FILE_DATA, new String(uploadRequest.getFileData()));

        ChatPostMessageRequest postRequest = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, postRequest.getToken());
        assertEquals(VALID_CHANNEL, postRequest.getChannel());
        assertNull(postRequest.getText());
        List<Attachment> attachments = postRequest.getAttachments();
        assertNotNull(attachments);
        assertEquals(1, attachments.size());
        String expected = "<" + BASE_PERMA_LINK + FILE_NAME + "|" + FILE_NAME + ">";
        assertEquals(expected, attachments.get(0).getText());
        assertEquals(FILE_NAME, attachments.get(0).getFilename());
        assertEquals(BASE_PERMA_LINK + FILE_NAME, attachments.get(0).getUrl());
        List<File> files = attachments.get(0).getFiles();
        assertNotNull(files);
        assertEquals(1, files.size());
        assertEquals(FILE_NAME, files.get(0).getName());
        assertEquals(FILE_NAME, files.get(0).getTitle());
        assertEquals(BASE_PERMA_LINK + FILE_NAME, files.get(0).getPermalink());
        assertEquals(BASE_URL_PRIVATE + FILE_NAME, files.get(0).getUrlPrivate());

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals(BASE_URL_PRIVATE + FILE_NAME, flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void sendMessageWithAllPropertiesSuccessfully() {
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_LINK);
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.UPLOAD_CHANNEL, VALID_CHANNEL_ID);
        testRunner.setProperty(PublishSlack.THREAD_TS, THREAD_TS);
        testRunner.setProperty(PublishSlack.TEXT, TEXT);
        testRunner.setProperty(PublishSlack.INITIAL_COMMENT, INITIAL_COMMENT);
        testRunner.setProperty(PublishSlack.FILE_TITLE, FILE_TITLE);
        testRunner.setProperty(PublishSlack.FILE_NAME, FILE_NAME);
        testRunner.setProperty("attachment_01", "{\"my-attachment-key\": \"my-attachment-value\"}");

        testRunner.enqueue(FILE_DATA);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        FilesUploadV2Request uploadRequest = mockClient.getLastFilesUploadRequest();
        assertEquals(VALID_TOKEN, uploadRequest.getToken());
        assertEquals(VALID_CHANNEL_ID, uploadRequest.getChannel());
        assertEquals(THREAD_TS, uploadRequest.getThreadTs());
        assertEquals(INITIAL_COMMENT, uploadRequest.getInitialComment());
        assertEquals(FILE_NAME, uploadRequest.getFilename());
        assertEquals(FILE_TITLE, uploadRequest.getTitle());
        assertEquals(FILE_DATA, new String(uploadRequest.getFileData()));

        ChatPostMessageRequest postRequest = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, postRequest.getToken());
        assertEquals(VALID_CHANNEL, postRequest.getChannel());
        String expected = TEXT + "\n<" + BASE_PERMA_LINK + FILE_NAME + "|" + FILE_TITLE + ">";
        assertEquals(expected, postRequest.getText());
        List<Attachment> attachments = postRequest.getAttachments();
        assertNotNull(attachments);
        assertEquals(1, attachments.size());
        assertEquals("{\"my-attachment-key\":\"my-attachment-value\"}", attachments.get(0).getText());

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals(BASE_URL_PRIVATE + FILE_NAME, flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void processShouldFailWhenChannelIsEmpty() {
        // default strategy is STRATEGY_UPLOAD_ATTACH, so we don't need to set it
        // testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ATTACH);
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, EMPTY_TEXT);

        testRunner.enqueue(FILE_DATA);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_FAILURE);
    }

    @Test
    public void fileNameShouldHaveFallbackValueWhenEmpty() {
        final String defaultFileName = "file";
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_LINK);
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.FILE_NAME, EMPTY_TEXT);

        testRunner.enqueue(FILE_DATA);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        FilesUploadV2Request uploadRequest = mockClient.getLastFilesUploadRequest();
        assertEquals(VALID_TOKEN, uploadRequest.getToken());
        assertNull(uploadRequest.getChannel());
        assertNull(uploadRequest.getThreadTs());
        assertNull(uploadRequest.getInitialComment());
        // fallback value for file name is 'file'
        assertEquals(defaultFileName, uploadRequest.getFilename());
        assertEquals(defaultFileName, uploadRequest.getTitle());
        assertEquals(FILE_DATA, new String(uploadRequest.getFileData()));

        ChatPostMessageRequest postRequest = mockClient.getLastChatPostMessageRequest();
        assertEquals(VALID_TOKEN, postRequest.getToken());
        assertEquals(VALID_CHANNEL, postRequest.getChannel());
        String expected = "<" + BASE_PERMA_LINK + defaultFileName + "|" + defaultFileName + ">";
        assertEquals(expected, postRequest.getText());
        List<Attachment> attachments = postRequest.getAttachments();
        assertNull(attachments);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals(BASE_URL_PRIVATE + defaultFileName, flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void uploadFieldsShouldHaveFallbackValuesWhenUploadOnly() {
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ONLY);
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL_ID);
        testRunner.setProperty(PublishSlack.THREAD_TS, THREAD_TS);
        testRunner.setProperty(PublishSlack.TEXT, TEXT);
        testRunner.setProperty(PublishSlack.FILE_NAME, FILE_NAME);

        testRunner.enqueue(FILE_DATA);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        FilesUploadV2Request uploadRequest = mockClient.getLastFilesUploadRequest();
        assertEquals(VALID_TOKEN, uploadRequest.getToken());
        // channel should be used as backup for upload channel
        assertEquals(VALID_CHANNEL_ID, uploadRequest.getChannel());
        assertEquals(THREAD_TS, uploadRequest.getThreadTs());
        // text should be used as backup for initial comment
        assertEquals(TEXT, uploadRequest.getInitialComment());
        assertEquals(FILE_NAME, uploadRequest.getFilename());
        assertEquals(FILE_NAME, uploadRequest.getTitle());
        assertEquals(FILE_DATA, new String(uploadRequest.getFileData()));

        ChatPostMessageRequest postRequest = mockClient.getLastChatPostMessageRequest();
        assertNull(postRequest);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals(BASE_URL_PRIVATE + FILE_NAME, flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void channelNameShouldBeTranslatedToChannelIdForUploadOnly() {
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ONLY);
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.FILE_NAME, FILE_NAME);

        testRunner.enqueue(FILE_DATA);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        FilesUploadV2Request uploadRequest = mockClient.getLastFilesUploadRequest();
        assertEquals(VALID_TOKEN, uploadRequest.getToken());
        // channel should be used as backup for upload channel and translated to Channel ID
        assertEquals(VALID_CHANNEL_ID, uploadRequest.getChannel());
        assertNull(uploadRequest.getThreadTs());
        assertEquals(FILE_NAME, uploadRequest.getFilename());
        assertEquals(FILE_NAME, uploadRequest.getTitle());
        assertEquals(FILE_DATA, new String(uploadRequest.getFileData()));

        ChatPostMessageRequest postRequest = mockClient.getLastChatPostMessageRequest();
        assertNull(postRequest);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals(BASE_URL_PRIVATE + FILE_NAME, flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void sendInternationalMessageSuccessfully() {
        final String internationalization = "Iñtërnâtiônàližætiøn";
        testRunner.setProperty(PublishSlack.PUBLISH_STRATEGY, PublishSlack.STRATEGY_UPLOAD_ONLY);
        testRunner.setProperty(PublishSlack.BOT_TOKEN, VALID_TOKEN);
        testRunner.setProperty(PublishSlack.CHANNEL, VALID_CHANNEL);
        testRunner.setProperty(PublishSlack.TEXT, internationalization);
        testRunner.setProperty(PublishSlack.FILE_TITLE, internationalization);

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PublishSlack.REL_SUCCESS);

        FilesUploadV2Request uploadRequest = mockClient.getLastFilesUploadRequest();
        assertEquals(VALID_TOKEN, uploadRequest.getToken());
        // channel should be used as backup for upload channel and translated to Channel ID
        assertEquals(VALID_CHANNEL_ID, uploadRequest.getChannel());
        assertNull(uploadRequest.getThreadTs());
        assertEquals(internationalization, uploadRequest.getInitialComment());
        assertNotNull(uploadRequest.getFilename());
        assertTrue(uploadRequest.getFilename().endsWith(".mockFlowFile"));    // 994266770762705.mockFlowFile ?
        assertEquals(internationalization, uploadRequest.getTitle());
        assertEquals(0, uploadRequest.getFileData().length);

        ChatPostMessageRequest postRequest = mockClient.getLastChatPostMessageRequest();
        assertNull(postRequest);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals(BASE_URL_PRIVATE + uploadRequest.getFilename(), flowFileOut.getAttribute("slack.file.url"));
    }

}
