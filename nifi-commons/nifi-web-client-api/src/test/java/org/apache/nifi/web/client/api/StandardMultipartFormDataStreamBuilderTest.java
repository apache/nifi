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
package org.apache.nifi.web.client.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardMultipartFormDataStreamBuilderTest {

    private static final Pattern MUTLIPART_FORM_DATA_PATTERN = Pattern.compile("^multipart/form-data; boundary=\"([a-zA-Z0-9-]+)\"$");

    private static final int BOUNDARY_GROUP = 1;

    private static final String PART_BOUNDARY = "\r\n--%s\r\n";

    private static final String LAST_BOUNDARY = "\r\n--%s--";

    private static final String UPLOADED = "uploaded";

    private static final String FIELD = "field";

    private static final String CONTENT_DISPOSITION_HEADER = "Content-Disposition: form-data; name=\"%s\"\r\n";

    private static final Charset HEADERS_CHARACTER_SET = StandardCharsets.US_ASCII;

    private static final byte[] UNICODE_ENCODED = new byte[]{-50, -111, -50, -87};

    private static final String UNICODE_STRING = new String(UNICODE_ENCODED, StandardCharsets.UTF_8);

    private StandardMultipartFormDataStreamBuilder builder;

    @BeforeEach
    void setBuilder() {
        builder = new StandardMultipartFormDataStreamBuilder();
    }

    @Test
    void testGetHttpContentType() {
        final HttpContentType httpContentType = builder.getHttpContentType();
        final String contentType = httpContentType.getContentType();

        assertNotNull(contentType);
        final Matcher matcher = MUTLIPART_FORM_DATA_PATTERN.matcher(contentType);
        assertTrue(matcher.matches());
    }

    @Test
    void testAddPartNameDisallowed() {
        final String uploaded = String.class.getName();
        final byte[] uploadedBytes = uploaded.getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, () -> builder.addPart(UNICODE_STRING, StandardHttpContentType.APPLICATION_OCTET_STREAM, uploadedBytes));
    }

    @Test
    void testBuildException() {
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void testBuildTextPlain() throws IOException {
        final String value = String.class.getName();
        final byte[] bytes = value.getBytes(HEADERS_CHARACTER_SET);

        final InputStream inputStream = builder.addPart(UPLOADED, StandardHttpContentType.TEXT_PLAIN, bytes).build();

        final String body = readInputStream(inputStream);
        assertContentDispositionFound(body, UPLOADED);

        final String boundary = getBoundary(builder);
        assertLastBoundaryFound(body, boundary);

        assertTrue(body.contains(value));
    }

    @Test
    void testBuildMultipleParts() throws IOException {
        final String uploaded = String.class.getName();
        final byte[] uploadedBytes = uploaded.getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(uploadedBytes);
        builder.addPart(UPLOADED, StandardHttpContentType.APPLICATION_OCTET_STREAM, inputStream);

        final String field = Integer.class.getName();
        final byte[] fieldBytes = field.getBytes(HEADERS_CHARACTER_SET);
        builder.addPart(FIELD, StandardHttpContentType.TEXT_PLAIN, fieldBytes);

        final InputStream stream = builder.build();

        final String body = readInputStream(stream);
        assertContentDispositionFound(body, UPLOADED);
        assertContentDispositionFound(body, FIELD);

        final String boundary = getBoundary(builder);
        assertPartBoundaryFound(body, boundary);
        assertLastBoundaryFound(body, boundary);

        assertTrue(body.contains(uploaded));
        assertTrue(body.contains(field));
    }

    private void assertContentDispositionFound(final String body, final String name) {
        final String contentDispositionHeader = CONTENT_DISPOSITION_HEADER.formatted(name);
        assertTrue(body.contains(contentDispositionHeader));
    }

    private void assertPartBoundaryFound(final String body, final String boundary) {
        final String partBoundary = PART_BOUNDARY.formatted(boundary);
        assertTrue(body.contains(partBoundary));
    }

    private void assertLastBoundaryFound(final String body, final String boundary) {
        final String lastBoundary = LAST_BOUNDARY.formatted(boundary);
        assertTrue(body.endsWith(lastBoundary));
    }

    private String getBoundary(final MultipartFormDataStreamBuilder builder) {
        final HttpContentType httpContentType = builder.getHttpContentType();
        final String contentType = httpContentType.getContentType();
        final Matcher boundaryMatcher = MUTLIPART_FORM_DATA_PATTERN.matcher(contentType);
        assertTrue(boundaryMatcher.matches());
        return boundaryMatcher.group(BOUNDARY_GROUP);
    }

    private String readInputStream(final InputStream inputStream) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        inputStream.transferTo(outputStream);
        inputStream.close();
        return outputStream.toString();
    }
}
