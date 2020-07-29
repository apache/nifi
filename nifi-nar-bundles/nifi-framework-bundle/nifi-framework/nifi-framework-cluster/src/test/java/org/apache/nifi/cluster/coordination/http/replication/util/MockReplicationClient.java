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

package org.apache.nifi.cluster.coordination.http.replication.util;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
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

import org.apache.nifi.cluster.coordination.http.replication.HttpReplicationClient;
import org.apache.nifi.cluster.coordination.http.replication.PreparedRequest;

public class MockReplicationClient implements HttpReplicationClient {
    private int status = 200;
    private Object responseEntity = null;
    private MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();

    public void setResponse(final int status, final Object responseEntity, final MultivaluedMap<String, String> headers) {
        this.status = status;
        this.responseEntity = responseEntity;
        this.headers = headers;
    }

    @Override
    public PreparedRequest prepareRequest(String method, Map<String, String> headers, Object entity) {
        return new PreparedRequest() {
            @Override
            public String getMethod() {
                return method;
            }

            @Override
            public Map<String, String> getHeaders() {
                return headers;
            }

            @Override
            public Object getEntity() {
                return entity;
            }
        };
    }

    @Override
    public Response replicate(PreparedRequest request, String uri) throws IOException {
        return new Response() {

            @Override
            public int getStatus() {
                return status;
            }

            @Override
            public StatusType getStatusInfo() {
                return Status.fromStatusCode(status);
            }

            @Override
            public Object getEntity() {
                return responseEntity;
            }

            @Override
            public <T> T readEntity(Class<T> entityType) {
                if (responseEntity == null) {
                    return null;
                }

                if (entityType.isAssignableFrom(responseEntity.getClass())) {
                    return entityType.cast(responseEntity);
                }

                throw new IllegalArgumentException("Cannot cast entity as " + entityType);
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
                return responseEntity != null;
            }

            @Override
            public boolean bufferEntity() {
                return true;
            }

            @Override
            public void close() {
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
                return 0;
            }

            @Override
            public Set<String> getAllowedMethods() {
                return Collections.emptySet();
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
                return URI.create(uri);
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
            @SuppressWarnings({"unchecked", "rawtypes"})
            public MultivaluedMap<String, Object> getMetadata() {
                return (MultivaluedMap) headers;
            }

            @Override
            public MultivaluedMap<String, String> getStringHeaders() {
                return headers;
            }

            @Override
            public String getHeaderString(String name) {
                return headers.getFirst(name);
            }
        };
    }

}
