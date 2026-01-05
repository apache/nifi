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
package org.apache.nifi.web.controller;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.DownloadableContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardContentViewerControllerTest {

    private static final String REF_PARAMETER = "ref";
    private static final String REF_URL = "https://localhost:8443";
    private static final String CLIENT_ID_PARAMETER = "clientId";
    private static final String FORMATTED_PARAMETER = "formatted";
    private static final String MIME_TYPE_DISPLAY_NAME = "mimeTypeDisplayName";
    private static final String XML_DISPLAY_NAME = "xml";

    private static final String FILENAME = "FlowFile";
    private static final String TEXT_XML = "text/xml";
    private static final String XML_DOCUMENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><document><element/></document>";
    private static final String XML_DOCUMENT_FORMATTED = "<document>%n  <element/>%n</document>%n".formatted();

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private ServletContext servletContext;

    @Mock
    private ContentAccess contentAccess;

    private final StandardContentViewerController controller = new StandardContentViewerController();

    @Test
    void testDoGetNotFormatted() throws IOException {
        final InputStream contentStream = new ByteArrayInputStream(XML_DOCUMENT.getBytes(StandardCharsets.UTF_8));
        final DownloadableContent downloadableContent = new DownloadableContent(FILENAME, TEXT_XML, contentStream);
        setDownloadableContent(downloadableContent);

        final MockServletOutputStream mockServletOutputStream = new MockServletOutputStream();
        when(response.getOutputStream()).thenReturn(mockServletOutputStream);

        controller.doGet(request, response);

        final String outputString = mockServletOutputStream.getOutputString();
        assertEquals(XML_DOCUMENT, outputString);
    }

    @Test
    void testDoGetFormattedXml() throws IOException {
        setDownloadableContentXml(XML_DOCUMENT);

        final MockServletOutputStream mockServletOutputStream = new MockServletOutputStream();
        when(response.getOutputStream()).thenReturn(mockServletOutputStream);

        controller.doGet(request, response);

        final String outputString = mockServletOutputStream.getOutputString();
        assertEquals(XML_DOCUMENT_FORMATTED, outputString);
    }

    @Test
    void testDoGetFormattedXmlPreformatted() throws IOException {
        setDownloadableContentXml(XML_DOCUMENT_FORMATTED);

        final MockServletOutputStream mockServletOutputStream = new MockServletOutputStream();
        when(response.getOutputStream()).thenReturn(mockServletOutputStream);

        controller.doGet(request, response);

        final String outputString = mockServletOutputStream.getOutputString();
        assertEquals(XML_DOCUMENT_FORMATTED, outputString);
    }

    private void setDownloadableContentXml(final String contentXml) {
        final InputStream contentStream = new ByteArrayInputStream(contentXml.getBytes(StandardCharsets.UTF_8));
        final DownloadableContent downloadableContent = new DownloadableContent(FILENAME, TEXT_XML, contentStream);
        setDownloadableContent(downloadableContent);

        when(request.getParameter(eq(FORMATTED_PARAMETER))).thenReturn(Boolean.TRUE.toString());
        when(request.getParameter(eq(MIME_TYPE_DISPLAY_NAME))).thenReturn(XML_DISPLAY_NAME);
        when(request.getParameter(eq(CLIENT_ID_PARAMETER))).thenReturn(UUID.randomUUID().toString());
    }

    private void setDownloadableContent(final DownloadableContent downloadableContent) {
        when(request.getServletContext()).thenReturn(servletContext);
        when(servletContext.getAttribute(eq(StandardContentViewerController.CONTENT_ACCESS_ATTRIBUTE))).thenReturn(contentAccess);
        when(request.getParameter(eq(REF_PARAMETER))).thenReturn(REF_URL);
        when(contentAccess.getContent(any())).thenReturn(downloadableContent);
    }

    private static class MockServletOutputStream extends ServletOutputStream {

        private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(final WriteListener writeListener) {

        }

        @Override
        public void write(final int b) {
            outputStream.write(b);
        }

        private String getOutputString() {
            return outputStream.toString(StandardCharsets.UTF_8);
        }
    }
}
