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

package org.apache.nifi.cluster.coordination.http.replication.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import jakarta.ws.rs.core.EntityTag;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Link;
import jakarta.ws.rs.core.Link.Builder;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Replicated extension of standard Response with HTTP properties returned
 */
public class ReplicatedResponse extends Response {
    private static final int MAXIMUM_BUFFER_SIZE = 1048576;
    private static final int CONTENT_LENGTH_UNKNOWN = -1;

    private final ObjectMapper codec;
    private final InputStream responseBody;
    private final MultivaluedMap<String, String> responseHeaders;
    private final URI location;
    private final int statusCode;
    private final Runnable closeCallback;
    private final int contentLength;

    private final JsonFactory jsonFactory = new JsonFactory();

    private final byte[] bufferedResponseBody;

    public ReplicatedResponse(
            final ObjectMapper codec,
            final InputStream responseBody,
            final MultivaluedMap<String, String> responseHeaders,
            final URI location,
            final int statusCode,
            final int contentLength,
            final Runnable closeCallback
    ) {
        this.codec = codec;
        this.responseBody = responseBody;
        this.responseHeaders = responseHeaders;
        this.location = location;
        this.statusCode = statusCode;
        this.closeCallback = closeCallback;

        if (contentLength == CONTENT_LENGTH_UNKNOWN || contentLength > MAXIMUM_BUFFER_SIZE) {
            // Avoid buffering unknown Content-Length or greater than maximum buffer size specified
            bufferedResponseBody = null;
            this.contentLength = CONTENT_LENGTH_UNKNOWN;
        } else {
            bufferedResponseBody = readResponseBody(responseBody, location, statusCode);
            this.contentLength = bufferedResponseBody.length;
        }
    }

    @Override
    public int getStatus() {
        return statusCode;
    }

    @Override
    public StatusType getStatusInfo() {
        return Status.fromStatusCode(getStatus());
    }

    @Override
    public Object getEntity() {
        final InputStream responseBodyStream = getResponseBodyStream();

        try {
            final JsonParser parser = jsonFactory.createParser(responseBodyStream);
            parser.setCodec(codec);
            return parser.readValueAs(Object.class);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to parse response", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readEntity(Class<T> entityType) {
        final InputStream responseBodyStream = getResponseBodyStream();

        if (InputStream.class.equals(entityType)) {
            return (T) responseBodyStream;
        }

        if (String.class.equals(entityType)) {
            try {
                final byte[] responseBytes = responseBodyStream.readAllBytes();
                return (T) new String(responseBytes, StandardCharsets.UTF_8);
            } catch (final IOException e) {
                throw new UncheckedIOException("Read Replicated Response Body to String failed for %s".formatted(location), e);
            }
        }

        try {
            final JsonParser parser = jsonFactory.createParser(responseBodyStream);
            parser.setCodec(codec);
            return parser.readValueAs(entityType);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to parse response as entity of type " + entityType, e);
        }
    }

    @Override
    public <T> T readEntity(GenericType<T> entityType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readEntity(Class<T> entityType, Annotation[] annotations) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T readEntity(GenericType<T> entityType, Annotation[] annotations) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasEntity() {
        return true;
    }

    @Override
    public boolean bufferEntity() {
        return true;
    }

    @Override
    public void close() {
        if (closeCallback != null) {
            closeCallback.run();
        }
    }

    @Override
    public MediaType getMediaType() {
        return MediaType.APPLICATION_JSON_TYPE;
    }

    @Override
    public Locale getLanguage() {
        return null;
    }

    @Override
    public int getLength() {
        return contentLength;
    }

    @Override
    public Set<String> getAllowedMethods() {
        final String allowHeader = getHeaderString("Allow");
        if (allowHeader == null || allowHeader.trim().isEmpty()) {
            return Collections.emptySet();
        }

        final Set<String> allowed = new HashSet<>();
        for (final String allow : allowHeader.split(",")) {
            final String trimmed = allow.trim().toUpperCase();
            if (!trimmed.isEmpty()) {
                allowed.add(trimmed);
            }
        }

        return allowed;
    }

    @Override
    public Map<String, NewCookie> getCookies() {
        return Collections.emptyMap();
    }

    @Override
    public EntityTag getEntityTag() {
        return null;
    }

    @Override
    public Date getDate() {
        return null;
    }

    @Override
    public Date getLastModified() {
        return null;
    }

    @Override
    public URI getLocation() {
        return location;
    }

    @Override
    public Set<Link> getLinks() {
        return Collections.emptySet();
    }

    @Override
    public boolean hasLink(String relation) {
        return false;
    }

    @Override
    public Link getLink(String relation) {
        return null;
    }

    @Override
    public Builder getLinkBuilder(String relation) {
        return null;
    }

    @Override
    public MultivaluedMap<String, Object> getMetadata() {
        return new MultivaluedHashMap<>();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public MultivaluedMap<String, Object> getHeaders() {
        return (MultivaluedMap) responseHeaders;
    }

    @Override
    public MultivaluedMap<String, String> getStringHeaders() {
        return responseHeaders;
    }

    @Override
    public String getHeaderString(final String name) {
        final String headerValue = responseHeaders.getFirst(name);
        if (headerValue != null) {
            return headerValue;
        }

        return responseHeaders.getFirst(name.toLowerCase());
    }

    private InputStream getResponseBodyStream() {
        final InputStream responseBodyStream;

        if (bufferedResponseBody == null) {
            responseBodyStream = responseBody;
        } else {
            responseBodyStream = new ByteArrayInputStream(bufferedResponseBody);
        }

        return responseBodyStream;
    }

    private static byte[] readResponseBody(final InputStream inputStream, final URI location, final int statusCode) {
        try {
            return inputStream.readAllBytes();
        } catch (final IOException e) {
            throw new UncheckedIOException("Buffering Replicated Response Body failed %s HTTP %d".formatted(location, statusCode), e);
        }
    }
}
