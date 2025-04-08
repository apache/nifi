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

/**
 * Enumeration of common and registered HTTP request and response header names
 */
public enum HttpHeaderName {

    /** RFC 7231 */
    ACCEPT("Accept"),

    /** RFC 7231 */
    ACCEPT_ENCODING("Accept-Encoding"),

    /** RFC 7231 */
    ACCEPT_LANGUAGE("Accept-Language"),

    /** RFC 7235 */
    AUTHORIZATION("Authorization"),

    /** RFC 7234 */
    CACHE_CONTROL("Cache-Control"),

    /** RFC 7230 */
    CONNECTION("Connection"),

    /** RFC 7230 */
    CONTENT_ENCODING("Content-Encoding"),

    /** RFC 7230 */
    CONTENT_LENGTH("Content-Length"),

    /** RFC 7231 */
    CONTENT_TYPE("Content-Type"),

    /** RFC 7234 */
    ETAG("ETag"),

    /** RFC 7234 */
    EXPIRES("Expires"),

    /** RFC 7231 */
    HOST("Host"),

    /** RFC 7232 */
    IF_MODIFIED_SINCE("If-Modified-Since"),

    /** RFC 7232 */
    IF_NONE_MATCH("If-None-Match"),

    /** RFC 7231 */
    LOCATION("Location"),

    /** RFC 7231 */
    PRAGMA("Pragma"),

    /** RFC 7231 */
    REFERER("Referer"),

    /** RFC 7233 */
    RANGE("Range"),

    /** RFC 7230 */
    TRANSFER_ENCODING("Transfer-Encoding"),

    /** RFC 7231 */
    USER_AGENT("User-Agent");

    private final String headerName;

    HttpHeaderName(final String headerName) {
        this.headerName = headerName;
    }

    public String getHeaderName() {
        return headerName;
    }

}
