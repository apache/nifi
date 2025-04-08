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
 * Enumeration of common and registered Media Types
 */
public enum MediaType {

    /** RFC 4329 */
    APPLICATION_JAVASCRIPT("application/javascript"),

    /** RFC 8259 (was RFC 4627) */
    APPLICATION_JSON("application/json"),

    /** RFC 7303 */
    APPLICATION_XML("application/xml"),

    /** RFC 2046 */
    APPLICATION_OCTET_STREAM("application/octet-stream"),

    /** HTML form data (not IANA-registered with a formal RFC) */
    APPLICATION_X_WWW_FORM_URLENCODED("application/x-www-form-urlencoded"),

    /** RFC 2388 */
    MULTIPART_FORM_DATA("multipart/form-data"),

    /** RFC 2046 */
    TEXT_HTML("text/html"),

    /** RFC 4180 (informal, CSV is not formally IANA-registered) */
    TEXT_CSV("text/csv"),

    /** RFC 2046 */
    TEXT_PLAIN("text/plain"),

    /** RFC 7303 */
    TEXT_XML("text/xml");

    private final String mediaType;

    MediaType(final String mediaType) {
        this.mediaType = mediaType;
    }

    public String getMediaType() {
        return mediaType;
    }

}
