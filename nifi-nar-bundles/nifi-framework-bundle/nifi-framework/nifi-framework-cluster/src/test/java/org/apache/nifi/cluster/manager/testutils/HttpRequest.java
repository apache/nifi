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

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;

/**
 * Encapsulates an HTTP request. The toString method returns the specification-compliant request.
 *
 */
public class HttpRequest {

    private String method;
    private String uri;
    private String rawUri;
    private String version;
    private String body;
    private String rawRequest;
    private Map<String, String> headers = new HashMap<>();
    private Map<String, List<String>> parameters = new HashMap<>();

    public static HttpRequestBuilder createFromRequestLine(final String requestLine) {
        return new HttpRequestBuilder(requestLine);
    }

    public String getBody() {
        return body;
    }

    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }

    public String getHeaderValue(final String header) {
        for (final Map.Entry<String, String> entry : getHeaders().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(header)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public String getMethod() {
        return method;
    }

    public Map<String, List<String>> getParameters() {
        final Map<String, List<String>> result = new HashMap<>();
        for (final Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            result.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    public String getUri() {
        return uri;
    }

    public String getRawUri() {
        return rawUri;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return rawRequest;
    }

    /**
     * A builder for constructing basic HTTP requests. It handles only enough of the HTTP specification to support basic unit testing, and it should not be used otherwise.
     */
    public static class HttpRequestBuilder {

        private String method;
        private String uri;
        private String rawUri;
        private String version;
        private Map<String, String> headers = new HashMap<>();
        private Map<String, List<String>> parameters = new HashMap<>();
        private int contentLength = 0;
        private String contentType;
        private String body = "";
        private StringBuilder rawRequest = new StringBuilder();

        private HttpRequestBuilder(final String requestLine) {

            final String[] tokens = requestLine.split(" ");
            if (tokens.length != 3) {
                throw new IllegalArgumentException("Invalid HTTP Request Line: " + requestLine);
            }

            method = tokens[0];
            if (HttpMethod.GET.equalsIgnoreCase(method) || HttpMethod.HEAD.equalsIgnoreCase(method) || HttpMethod.DELETE.equalsIgnoreCase(method) || HttpMethod.OPTIONS.equalsIgnoreCase(method)) {
                final int queryIndex = tokens[1].indexOf("?");
                if (queryIndex > -1) {
                    uri = tokens[1].substring(0, queryIndex);
                    addParameters(tokens[1].substring(queryIndex + 1));
                } else {
                    uri = tokens[1];
                }
            }
            rawUri = tokens[1];
            version = tokens[2];
            rawRequest.append(requestLine).append("\n");
        }

        private void addHeader(final String key, final String value) {
            if (key.contains(" ")) {
                throw new IllegalArgumentException("Header key may not contain spaces.");
            } else if ("content-length".equalsIgnoreCase(key)) {
                contentLength = (StringUtils.isBlank(value.trim())) ? 0 : Integer.parseInt(value.trim());
            } else if ("content-type".equalsIgnoreCase(key)) {
                contentType = value.trim();
            }
            headers.put(key, value);
        }

        public void addHeader(final String header) {
            final int firstColonIndex = header.indexOf(":");
            if (firstColonIndex < 0) {
                throw new IllegalArgumentException("Invalid HTTP Header line: " + header);
            }
            addHeader(header.substring(0, firstColonIndex), header.substring(firstColonIndex + 1));
            rawRequest.append(header).append("\n");
        }

        // final because constructor calls it
        public final void addParameters(final String queryString) {

            if (StringUtils.isBlank(queryString)) {
                return;
            }

            final String normQueryString;
            if (queryString.startsWith("?")) {
                normQueryString = queryString.substring(1);
            } else {
                normQueryString = queryString;
            }
            final String[] keyValuePairs = normQueryString.split("&");
            for (final String keyValuePair : keyValuePairs) {
                final String[] keyValueTokens = keyValuePair.split("=");
                try {
                    addParameter(
                            URLDecoder.decode(keyValueTokens[0], "utf-8"),
                            URLDecoder.decode(keyValueTokens[1], "utf-8")
                    );
                } catch (UnsupportedEncodingException use) {
                    throw new RuntimeException(use);
                }
            }
        }

        public void addParameter(final String key, final String value) {

            if (key.contains(" ")) {
                throw new IllegalArgumentException("Parameter key may not contain spaces: " + key);
            }

            final List<String> values;
            if (parameters.containsKey(key)) {
                values = parameters.get(key);
            } else {
                values = new ArrayList<>();
                parameters.put(key, values);
            }
            values.add(value);
        }

        public void addBody(final Reader reader) throws IOException {

            if (contentLength <= 0) {
                return;
            }

            final char[] buf = new char[contentLength];
            int offset = 0;
            int numRead = 0;
            while (offset < buf.length && (numRead = reader.read(buf, offset, buf.length - offset)) >= 0) {
                offset += numRead;
            }
            body = new String(buf);
            rawRequest.append("\n");
            rawRequest.append(body);
        }

        public HttpRequest build() throws UnsupportedEncodingException {

            if (HttpMethod.GET.equalsIgnoreCase(method) == false && HttpMethod.HEAD.equalsIgnoreCase(method) == false && contentType.equalsIgnoreCase(MediaType.APPLICATION_FORM_URLENCODED)) {
                addParameters(body);
            }

            final HttpRequest request = new HttpRequest();
            request.method = this.method;
            request.uri = this.uri;
            request.rawUri = this.rawUri;
            request.version = this.version;
            request.headers.putAll(this.headers);
            request.parameters.putAll(this.parameters);
            request.body = this.body;
            request.rawRequest = this.rawRequest.toString();

            return request;
        }

    }
}
