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

public enum HttpHeaderName {

    // Authentication & Authorization
    AUTHORIZATION("Authorization"),
    BEARER("Bearer "), // Usually used as prefix in Authorization header

    // Content negotiation
    ACCEPT("Accept"),
    ACCEPT_ENCODING("Accept-Encoding"),
    ACCEPT_LANGUAGE("Accept-Language"),
    CONTENT_TYPE("Content-Type"),
    CONTENT_LENGTH("Content-Length"),
    CONTENT_ENCODING("Content-Encoding"),
    TRANSFER_ENCODING("Transfer-Encoding"),

    // Caching
    CACHE_CONTROL("Cache-Control"),
    PRAGMA("Pragma"),
    EXPIRES("Expires"),
    ETAG("ETag"),
    IF_MODIFIED_SINCE("If-Modified-Since"),
    IF_NONE_MATCH("If-None-Match"),

    // Connection / Transport
    CONNECTION("Connection"),
    HOST("Host"),
    ORIGIN("Origin"),
    REFERER("Referer"),
    USER_AGENT("User-Agent"),

    // Request specifics
    RANGE("Range"),
    COOKIE("Cookie"),
    SET_COOKIE("Set-Cookie"),
    LOCATION("Location"),

    // Security-related
    X_FORWARDED_FOR("X-Forwarded-For"),
    X_FORWARDED_PROTO("X-Forwarded-Proto"),
    X_FRAME_OPTIONS("X-Frame-Options"),
    X_CONTENT_TYPE_OPTIONS("X-Content-Type-Options"),
    STRICT_TRANSPORT_SECURITY("Strict-Transport-Security"),

    // CORS
    ACCESS_CONTROL_ALLOW_ORIGIN("Access-Control-Allow-Origin"),
    ACCESS_CONTROL_ALLOW_METHODS("Access-Control-Allow-Methods"),
    ACCESS_CONTROL_ALLOW_HEADERS("Access-Control-Allow-Headers"),
    ACCESS_CONTROL_EXPOSE_HEADERS("Access-Control-Expose-Headers"),
    ACCESS_CONTROL_MAX_AGE("Access-Control-Max-Age"),
    ACCESS_CONTROL_REQUEST_METHOD("Access-Control-Request-Method"),
    ACCESS_CONTROL_REQUEST_HEADERS("Access-Control-Request-Headers"),

    // Tracing / Monitoring
    TRACEPARENT("traceparent"),
    REQUEST_ID("X-Request-ID"),
    CORRELATION_ID("X-Correlation-ID");

    private final String headerName;

    HttpHeaderName(final String headerName) {
        this.headerName = headerName;
    }

    public String getHeaderName() {
        return headerName;
    }

}
