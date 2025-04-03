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

public enum ContentType {

    APPLICATION_JSON("application/json"),
    APPLICATION_XML("application/xml"),
    APPLICATION_OCTET_STREAM("application/octet-stream"),
    APPLICATION_X_WWW_FORM_URLENCODED("application/x-www-form-urlencoded"),
    MULTIPART_FORM_DATA("multipart/form-data"),

    TEXT_PLAIN("text/plain"),
    TEXT_HTML("text/html"),
    TEXT_CSV("text/csv"),
    TEXT_XML("text/xml"),

    IMAGE_PNG("image/png"),
    IMAGE_JPEG("image/jpeg"),
    IMAGE_GIF("image/gif"),
    IMAGE_WEBP("image/webp"),
    IMAGE_SVG_XML("image/svg+xml"),

    AUDIO_MPEG("audio/mpeg"),
    AUDIO_OGG("audio/ogg"),

    VIDEO_MP4("video/mp4"),
    VIDEO_WEBM("video/webm"),

    APPLICATION_PDF("application/pdf"),
    APPLICATION_ZIP("application/zip"),
    APPLICATION_GZIP("application/gzip"),
    APPLICATION_JAVASCRIPT("application/javascript"),
    APPLICATION_PROBLEM_JSON("application/problem+json"), // For RFC 7807 problem details
    APPLICATION_JSON_PATCH("application/json-patch+json"), // For PATCH requests

    APPLICATION_VND_API_JSON("application/vnd.api+json"); // JSON:API media type

    private final String contentType;

    ContentType(final String contentType) {
        this.contentType = contentType;
    }

    public String getContentType() {
        return contentType;
    }

}
