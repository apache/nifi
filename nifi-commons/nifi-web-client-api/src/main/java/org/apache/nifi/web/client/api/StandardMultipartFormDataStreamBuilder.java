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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation of Multipart Form Data Stream Builder supporting form-data as described in RFC 7578
 */
public class StandardMultipartFormDataStreamBuilder implements MultipartFormDataStreamBuilder {
    private static final String CONTENT_DISPOSITION_HEADER = "Content-Disposition: form-data; name=\"%s\"";
    private static final String CONTENT_DISPOSITION_FILE_HEADER = "Content-Disposition: form-data; name=\"%s\"; filename=\"%s\"";

    private static final String CONTENT_TYPE_HEADER = "Content-Type: %s";

    private static final Pattern ALLOWED_NAME_PATTERN = Pattern.compile("^\\p{ASCII}+$");

    private static final String CARRIAGE_RETURN_LINE_FEED = "\r\n";

    private static final String BOUNDARY_SEPARATOR = "--";

    private static final String BOUNDARY_FORMAT = "FormDataBoundary-%s";

    private static final String MULTIPART_FORM_DATA_FORMAT = "multipart/form-data; boundary=\"%s\"";

    private static final Charset HEADERS_CHARACTER_SET = StandardCharsets.US_ASCII;

    private final String boundary = BOUNDARY_FORMAT.formatted(UUID.randomUUID());

    private final List<Part> parts = new ArrayList<>();

    @Override
    public InputStream build() {
        if (parts.isEmpty()) {
            throw new IllegalStateException("Parts required");
        }

        final List<InputStream> streams = new ArrayList<>();
        for (int index = 0; index < parts.size(); index++) {
            final Part part = parts.get(index);
            final String boundaryPrefix = getBoundaryPrefix(index);
            streams.add(new ByteArrayInputStream(boundaryPrefix.getBytes(HEADERS_CHARACTER_SET)));
            final String partHeaders = getPartHeaders(part);
            streams.add(new ByteArrayInputStream(partHeaders.getBytes(HEADERS_CHARACTER_SET)));
            streams.add(part.inputStream);
        }

        streams.add(new ByteArrayInputStream(getFooter().getBytes(HEADERS_CHARACTER_SET)));

        final Enumeration<InputStream> enumeratedStreams = Collections.enumeration(streams);
        return new SequenceInputStream(enumeratedStreams);
    }

    /**
     * Get Content-Type Header value containing multipart/form-data with boundary
     *
     * @return Multipart HTTP Content-Type
     */
    @Override
    public HttpContentType getHttpContentType() {
        final String contentType = MULTIPART_FORM_DATA_FORMAT.formatted(boundary);
        return new MultipartHttpContentType(contentType);
    }

    /**
     * Add Part with field name and stream source
     *
     * @param name Name field of part to be added
     * @param httpContentType Content-Type of part to be added
     * @param inputStream Stream content of part to be added
     * @return Builder
     */
    @Override
    public MultipartFormDataStreamBuilder addPart(final String name, final HttpContentType httpContentType, final InputStream inputStream) {
        return addPartInternal(name, null, httpContentType, inputStream);
    }

    @Override
    public MultipartFormDataStreamBuilder addPart(final String name, final String fileName, final HttpContentType httpContentType, final InputStream inputStream) {
        final String sanitizedFileName = sanitizeFileName(fileName);
        return addPartInternal(name, sanitizedFileName, httpContentType, inputStream);
    }

    private MultipartFormDataStreamBuilder addPartInternal(final String name, final String fileName, final HttpContentType httpContentType, final InputStream inputStream) {
        Objects.requireNonNull(name, "Name required");
        Objects.requireNonNull(httpContentType, "Content Type required");
        Objects.requireNonNull(inputStream, "Input Stream required");

        final Matcher nameMatcher = ALLOWED_NAME_PATTERN.matcher(name);
        if (nameMatcher.matches()) {
            final Part part = new Part(name, fileName, httpContentType, inputStream);
            parts.add(part);
        } else {
            throw new IllegalArgumentException("Name contains characters outside of ASCII character set");
        }

        return this;
    }

    /**
     * Add Part with field name and byte array source
     *
     * @param name Name field of part to be added
     * @param httpContentType Content-Type of part to be added
     * @param bytes Byte array content of part to be added
     * @return Builder
     */
    @Override
    public MultipartFormDataStreamBuilder addPart(final String name, final HttpContentType httpContentType, final byte[] bytes) {
        Objects.requireNonNull(bytes, "Byte Array required");
        final InputStream inputStream = new ByteArrayInputStream(bytes);
        return addPart(name, httpContentType, inputStream);
    }

    @Override
    public MultipartFormDataStreamBuilder addPart(final String name, final String fileName, final HttpContentType httpContentType, final byte[] bytes) {
        Objects.requireNonNull(bytes, "Byte Array required");
        final InputStream inputStream = new ByteArrayInputStream(bytes);
        return addPart(name, fileName, httpContentType, inputStream);
    }

    private String getPartHeaders(final Part part) {
        final StringBuilder headersBuilder = new StringBuilder();

        final String contentDispositionHeader = getContentDispositionHeader(part);
        headersBuilder.append(contentDispositionHeader);
        headersBuilder.append(CARRIAGE_RETURN_LINE_FEED);

        final String contentType = part.httpContentType.getContentType();
        final String contentTypeHeader = CONTENT_TYPE_HEADER.formatted(contentType);
        headersBuilder.append(contentTypeHeader);
        headersBuilder.append(CARRIAGE_RETURN_LINE_FEED);

        headersBuilder.append(CARRIAGE_RETURN_LINE_FEED);
        return headersBuilder.toString();
    }

    private String getBoundaryPrefix(final int index) {
        final StringBuilder prefixBuilder = new StringBuilder();
        if (index > 0) {
            prefixBuilder.append(CARRIAGE_RETURN_LINE_FEED);
        }
        prefixBuilder.append(BOUNDARY_SEPARATOR);
        prefixBuilder.append(boundary);
        prefixBuilder.append(CARRIAGE_RETURN_LINE_FEED);
        return prefixBuilder.toString();
    }

    private String getFooter() {
        return CARRIAGE_RETURN_LINE_FEED + BOUNDARY_SEPARATOR + boundary + BOUNDARY_SEPARATOR;
    }

    private String getContentDispositionHeader(final Part part) {
        if (part.fileName == null) {
            return CONTENT_DISPOSITION_HEADER.formatted(part.name);
        }
        return CONTENT_DISPOSITION_FILE_HEADER.formatted(part.name, part.fileName);
    }

    private record MultipartHttpContentType(String contentType) implements HttpContentType {
        @Override
        public String getContentType() {
            return contentType;
        }
    }

    private record Part(
            String name,
            String fileName,
            HttpContentType httpContentType,
            InputStream inputStream
    ) {
    }

    private String sanitizeFileName(final String fileName) {
        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("File Name required");
        }

        final String sanitized = fileName;
        final Matcher fileNameMatcher = ALLOWED_NAME_PATTERN.matcher(sanitized);
        if (!fileNameMatcher.matches()) {
            throw new IllegalArgumentException("File Name contains characters outside of ASCII character set");
        }

        return sanitized;
    }
}
