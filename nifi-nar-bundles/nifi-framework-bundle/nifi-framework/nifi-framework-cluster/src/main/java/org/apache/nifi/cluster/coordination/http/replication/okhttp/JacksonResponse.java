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

package org.apache.nifi.cluster.coordination.http.replication.okhttp;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.Link.Builder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonResponse extends Response {
    private final ObjectMapper codec;
    private final byte[] responseBody;
    private final MultivaluedMap<String, String> responseHeaders;
    private final URI location;
    private final int statusCode;
    private final Runnable closeCallback;

    private final JsonFactory jsonFactory = new JsonFactory();

    public JacksonResponse(final ObjectMapper codec, final byte[] responseBody, final MultivaluedMap<String, String> responseHeaders, final URI location, final int statusCode,
            final Runnable closeCallback) {
        this.codec = codec;
        this.responseBody = responseBody;
        this.responseHeaders = responseHeaders;
        this.location = location;
        this.statusCode = statusCode;
        this.closeCallback = closeCallback;
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
        try {
            final JsonParser parser = jsonFactory.createParser(responseBody);
            parser.setCodec(codec);
            return parser.readValueAs(Object.class);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to parse response", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readEntity(Class<T> entityType) {
        if (InputStream.class.equals(entityType)) {
            return (T) new ByteArrayInputStream(responseBody);
        }

        if (String.class.equals(entityType)) {
            return (T) new String(responseBody, StandardCharsets.UTF_8);
        }

        try {
            final JsonParser parser = jsonFactory.createParser(responseBody);
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
        return responseBody != null && responseBody.length > 0;
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
        return responseBody == null ? 0 : responseBody.length;
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
    public String getHeaderString(String name) {
        return responseHeaders.getFirst(name);
    }
}
