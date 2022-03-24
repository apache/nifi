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
package org.apache.nifi.cluster.manager.testutils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

/**
 * Encapsulates an HTTP response. The toString method returns the specification-compliant response.
 *
 */
public class HttpResponse {

    private final Status status;
    private final String entity;
    private final Map<String, String> headers = new HashMap<>();

    public HttpResponse(final Status status, final String entity) {
        this.status = status;
        this.entity = entity;
        headers.put("content-length", String.valueOf(entity.getBytes().length));
    }

    public String getEntity() {
        return entity;
    }

    public Status getStatus() {
        return status;
    }

    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }

    public void addHeader(final String key, final String value) {
        if (key.contains(" ")) {
            throw new IllegalArgumentException("Header key may not contain spaces.");
        } else if ("content-length".equalsIgnoreCase(key)) {
            throw new IllegalArgumentException("Content-Length header is set automatically based on length of content.");
        }
        headers.put(key, value);
    }

    public void addHeaders(final Map<String, String> headers) {
        for (final Map.Entry<String, String> entry : headers.entrySet()) {
            addHeader(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String toString() {

        final StringBuilder strb = new StringBuilder();

        // response line
        strb.append("HTTP/1.1 ")
                .append(status.getStatusCode())
                .append(" ")
                .append(status.getReasonPhrase())
                .append("\n");

        // headers
        for (final Map.Entry<String, String> entry : headers.entrySet()) {
            strb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }

        strb.append("\n");

        // body
        strb.append(entity);

        return strb.toString();
    }
}
