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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.TestServer;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.nifi.processors.slack.PostSlackCaptureServlet.REQUEST_PATH_SUCCESS_FILE_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PostSlackFileMessageTest {

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
    public void sendMessageWithBasicPropertiesSuccessfully() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, server.getUrl() + REQUEST_PATH_SUCCESS_FILE_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);

        Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), "my-file-name");
        flowFileAttributes.put(CoreAttributes.MIME_TYPE.key(), "image/png");

        // in order not to make the assertion logic (even more) complicated, the file content is tested with character data instead of binary data
        testRunner.enqueue("my-data", flowFileAttributes);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        assertRequest("my-file-name", "image/png", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void sendMessageWithAllPropertiesSuccessfully() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, server.getUrl() + REQUEST_PATH_SUCCESS_FILE_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "my-text");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_TITLE, "my-file-title");
        testRunner.setProperty(PostSlack.FILE_NAME, "my-file-name");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "image/png");

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        assertRequest("my-file-name", "image/png", "my-text", "my-file-title");

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void processShouldFailWhenChannelIsEmpty() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, server.getUrl());
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "${dummy}");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_FAILURE);

        assertFalse(servlet.hasBeenInteracted());
    }

    @Test
    public void fileNameShouldHaveFallbackValueWhenEmpty() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, server.getUrl() + REQUEST_PATH_SUCCESS_FILE_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_NAME, "${dummy}");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "image/png");

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        // fallback value for file name is 'file'
        assertRequest("file", "image/png", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void mimeTypeShouldHaveFallbackValueWhenEmpty() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, server.getUrl() + REQUEST_PATH_SUCCESS_FILE_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_NAME, "my-file-name");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "${dummy}");

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        // fallback value for mime type is 'application/octet-stream'
        assertRequest("my-file-name", "application/octet-stream", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void mimeTypeShouldHaveFallbackValueWhenInvalid() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, server.getUrl() + REQUEST_PATH_SUCCESS_FILE_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_NAME, "my-file-name");
        testRunner.setProperty(PostSlack.FILE_MIME_TYPE, "invalid");

        testRunner.enqueue("my-data");
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        // fallback value for mime type is 'application/octet-stream'
        assertRequest("my-file-name", "application/octet-stream", null, null);

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    @Test
    public void sendInternationalMessageSuccessfully() {
        testRunner.setProperty(PostSlack.FILE_UPLOAD_URL, server.getUrl() + REQUEST_PATH_SUCCESS_FILE_MSG);
        testRunner.setProperty(PostSlack.ACCESS_TOKEN, "my-access-token");
        testRunner.setProperty(PostSlack.CHANNEL, "my-channel");
        testRunner.setProperty(PostSlack.TEXT, "Iñtërnâtiônàližætiøn");
        testRunner.setProperty(PostSlack.UPLOAD_FLOWFILE, PostSlack.UPLOAD_FLOWFILE_YES);
        testRunner.setProperty(PostSlack.FILE_TITLE, "Iñtërnâtiônàližætiøn");

        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(PutSlack.REL_SUCCESS);

        Map<String, String> parts = parsePostBodyParts(parseMultipartBoundary(servlet.getLastPostHeaders().get("Content-Type")));
        assertEquals("Iñtërnâtiônàližætiøn", parts.get("initial_comment"));
        assertEquals("Iñtërnâtiônàližætiøn", parts.get("title"));

        FlowFile flowFileOut = testRunner.getFlowFilesForRelationship(PutSlack.REL_SUCCESS).get(0);
        assertEquals("slack-file-url", flowFileOut.getAttribute("slack.file.url"));
    }

    private void assertRequest(String fileName, String mimeType, String text, String title) {
        Map<String, String> requestHeaders = servlet.getLastPostHeaders();
        assertEquals("Bearer my-access-token", requestHeaders.get("Authorization"));

        String contentType = requestHeaders.get("Content-Type");
        assertTrue(contentType.startsWith("multipart/form-data"));

        String boundary = parseMultipartBoundary(contentType);
        assertNotNull("Multipart boundary not found in Content-Type header: " + contentType, boundary);

        Map<String, String> parts = parsePostBodyParts(boundary);

        assertNotNull("'channels' parameter not found in the POST request body", parts.get("channels"));
        assertEquals("'channels' parameter has wrong value", "my-channel", parts.get("channels"));

        if (text != null) {
            assertNotNull("'initial_comment' parameter not found in the POST request body", parts.get("initial_comment"));
            assertEquals("'initial_comment' parameter has wrong value", text, parts.get("initial_comment"));
        }

        assertNotNull("'filename' parameter not found in the POST request body", parts.get("filename"));
        assertEquals("'fileName' parameter has wrong value", fileName, parts.get("filename"));

        if (title != null) {
            assertNotNull("'title' parameter not found in the POST request body", parts.get("title"));
            assertEquals("'title' parameter has wrong value", title, parts.get("title"));
        }

        assertNotNull("The file part not found in the POST request body", parts.get("file"));

        Map<String, String> fileParameters = parseFilePart(boundary);
        assertEquals("File data is wrong in the POST request body", "my-data", fileParameters.get("data"));
        assertEquals("'filename' attribute of the file part has wrong value", fileName, fileParameters.get("filename"));
        assertEquals("Content-Type of the file part is wrong", mimeType, fileParameters.get("contentType"));
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

    private Map<String, String> parsePostBodyParts(String boundary) {
        Pattern partNamePattern = Pattern.compile("name=\"(.*?)\"");
        Pattern partDataPattern = Pattern.compile("\r\n\r\n(.*?)\r\n$");

        String[] postBodyParts = new String(servlet.getLastPostBody(), Charset.forName("UTF-8")).split(boundary);

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

    private Map<String, String> parseFilePart(String boundary) {
        Pattern partNamePattern = Pattern.compile("name=\"file\"");
        Pattern partDataPattern = Pattern.compile("\r\n\r\n(.*?)\r\n$");
        Pattern partFilenamePattern = Pattern.compile("filename=\"(.*?)\"");
        Pattern partContentTypePattern = Pattern.compile("Content-Type: (.*?)\r\n");

        String[] postBodyParts = new String(servlet.getLastPostBody(), Charset.forName("UTF-8")).split(boundary);

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
