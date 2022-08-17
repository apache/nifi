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

import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostSlackFileMessageTest {

    private static final String RESPONSE_SUCCESS_FILE_MSG = "{\"ok\": true, \"file\": {\"url_private\": \"slack-file-url\"}}";

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
    public void sendMessageWithBasicPropertiesSuccessfully() throws InterruptedException {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);

        Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), "my-file-name");
        flowFileAttributes.put(CoreAttributes.MIME_TYPE.key(), "image/png");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

        // in order not to make the assertion logic (even more) complicated, the file content is tested with character data instead of binary data
        testRunner.enqueue("my-data", flowFileAttributes);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        assertRequest("my-file-name", "image/png", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void sendMessageWithAllPropertiesSuccessfully() throws InterruptedException {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_TITLE, "my-file-title");
        testRunner.setProperty(PostSlack.FILE_NAME, "my-file-name");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "image/png");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        assertRequest("my-file-name", "image/png", "my-text", "my-file-title");

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void processShouldFailWhenChannelIsEmpty() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "${dummy}");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);
    }

    @Test
    public void fileNameShouldHaveFallbackValueWhenEmpty() throws InterruptedException {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_NAME, "${dummy}");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "image/png");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        // fallback value for file name is 'file'
        assertRequest("file", "image/png", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void mimeTypeShouldHaveFallbackValueWhenEmpty() throws InterruptedException {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_NAME, "my-file-name");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "${dummy}");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        // fallback value for mime type is 'application/octet-stream'
        assertRequest("my-file-name", "application/octet-stream", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void mimeTypeShouldHaveFallbackValueWhenInvalid() throws InterruptedException {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_NAME, "my-file-name");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "invalid");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        // fallback value for mime type is 'application/octet-stream'
        assertRequest("my-file-name", "application/octet-stream", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void sendInternationalMessageSuccessfully() throws InterruptedException {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, url);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "Iñtërnâtiônàližætiøn");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_TITLE, "Iñtërnâtiônàližætiøn");

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_FILE_MSG));

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        final RecordedRequest recordedRequest = mockWebServer.takeRequest();
        final String body = recordedRequest.getBody().readString(StandardCharsets.UTF_8);

        final Headers headers = recordedRequest.getHeaders();

        Map<String, String> parts = parsePostBodyParts(parseMultipartBoundary(headers.get("Content-Type")), body);
        assertEquals("Iñtërnâtiônàližætiøn", parts.get("initial_comment"));
        assertEquals("Iñtërnâtiônàližætiøn", parts.get("title"));

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    private void assertRequest(String fileName, String mimeType, String text, String title) throws InterruptedException {
        final RecordedRequest recordedRequest = mockWebServer.takeRequest();
        final String body = recordedRequest.getBody().readString(StandardCharsets.UTF_8);

        final Headers headers = recordedRequest.getHeaders();
        assertEquals("Bearer my-access-token", headers.get("Authorization"));

        String contentType = headers.get("Content-Type");
        assertTrue(contentType.startsWith("multipart/form-data"));

        String boundary = parseMultipartBoundary(contentType);
        assertNotNull(boundary, "Multipart boundary not found in Content-Type header: " + contentType);

        Map<String, String> parts = parsePostBodyParts(boundary, body);

        assertNotNull(parts.get("channels"), "'channels' parameter not found in the POST request body");
        assertEquals("my-channel", parts.get("channels"), "'channels' parameter has wrong value");

        if (text != null) {
            assertNotNull(parts.get("initial_comment"), "'initial_comment' parameter not found in the POST request body");
            assertEquals(text, parts.get("initial_comment"), "'initial_comment' parameter has wrong value");
        }

        assertNotNull(parts.get("filename"), "'filename' parameter not found in the POST request body");
        assertEquals(fileName, parts.get("filename"), "'fileName' parameter has wrong value");

        if (title != null) {
            assertNotNull(parts.get("title"), "'title' parameter not found in the POST request body");
            assertEquals(title, parts.get("title"), "'title' parameter has wrong value");
        }

        assertNotNull(parts.get("file"), "The file part not found in the POST request body");


        Map<String, String> fileParameters = parseFilePart(boundary, body);
        assertEquals("my-data", fileParameters.get("data"), "File data is wrong in the POST request body");
        assertEquals(fileName, fileParameters.get("filename"), "'filename' attribute of the file part has wrong value");
        assertEquals(mimeType, fileParameters.get("contentType"), "Content-Type of the file part is wrong");
    }

    private String parseMultipartBoundary(String contentType) {
        String boundary = null;

        Pattern boundaryPattern = Pattern.compile("boundary=(.*?)$");
        Matcher boundaryMatcher = boundaryPattern.matcher(contentType);

        if (boundaryMatcher.find()) {
            boundary = "--" + boundaryMatcher.group(1);
        }

        return boundary;
    }

    private Map<String, String> parsePostBodyParts(String boundary, String body) {
        Pattern partNamePattern = Pattern.compile("name=\"(.*?)\"");
        Pattern partDataPattern = Pattern.compile("\r\n\r\n(.*?)\r\n$");

        String[] postBodyParts = body.split(boundary);

        Map<String, String> parts = new HashMap<>();

        for (String part: postBodyParts) {
            Matcher partNameMatcher = partNamePattern.matcher(part);
            Matcher partDataMatcher = partDataPattern.matcher(part);

            if (partNameMatcher.find() && partDataMatcher.find()) {
                String partName = partNameMatcher.group(1);
                String partData = partDataMatcher.group(1);

                parts.put(partName, partData);
            }
        }

        return parts;
    }

    private Map<String, String> parseFilePart(String boundary, String body) {
        Pattern partNamePattern = Pattern.compile("name=\"file\"");
        Pattern partDataPattern = Pattern.compile("\r\n\r\n(.*?)\r\n$");
        Pattern partFilenamePattern = Pattern.compile("filename=\"(.*?)\"");
        Pattern partContentTypePattern = Pattern.compile("Content-Type: (.*?)\r\n");

        String[] postBodyParts = body.split(boundary);

        Map<String, String> fileParameters = new HashMap<>();

        for (String part: postBodyParts) {
            Matcher partNameMatcher = partNamePattern.matcher(part);

            if (partNameMatcher.find()) {
                Matcher partDataMatcher = partDataPattern.matcher(part);
                if (partDataMatcher.find()) {
                    fileParameters.put("data", partDataMatcher.group(1));
                }

                Matcher partFilenameMatcher = partFilenamePattern.matcher(part);
                if (partFilenameMatcher.find()) {
                    fileParameters.put("filename", partFilenameMatcher.group(1));
                }

                Matcher partContentTypeMatcher = partContentTypePattern.matcher(part);
                if (partContentTypeMatcher.find()) {
                    fileParameters.put("contentType", partContentTypeMatcher.group(1));
                }
            }
        }

        return fileParameters;
    }
}
