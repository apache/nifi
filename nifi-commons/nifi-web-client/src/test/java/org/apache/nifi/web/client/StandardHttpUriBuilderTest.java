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
package org.apache.nifi.web.client;

import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandardHttpUriBuilderTest {
    private static final String HTTP_SCHEME = "http";

    private static final String LOCALHOST = "localhost";

    private static final int PORT = 8080;

    private static final String ENCODED_PATH = "/resources/search";

    private static final String RESOURCES_PATH_SEGMENT = "resources";

    private static final String PARAMETER_NAME = "search";

    private static final String PARAMETER_VALUE = "terms";

    private static final URI HTTP_LOCALHOST_URI = URI.create(String.format("%s://%s/", HTTP_SCHEME, LOCALHOST));

    private static final URI HTTP_LOCALHOST_PORT_URI = URI.create(String.format("%s://%s:%d/", HTTP_SCHEME, LOCALHOST, PORT));

    private static final URI HTTP_LOCALHOST_PORT_ENCODED_PATH_URI = URI.create(String.format("%s://%s:%d%s", HTTP_SCHEME, LOCALHOST, PORT, ENCODED_PATH));

    private static final URI HTTP_LOCALHOST_RESOURCES_URI = URI.create(String.format("%s%s", HTTP_LOCALHOST_URI, RESOURCES_PATH_SEGMENT));

    private static final URI HTTP_LOCALHOST_QUERY_URI = URI.create(String.format("%s?%s=%s", HTTP_LOCALHOST_RESOURCES_URI, PARAMETER_NAME, PARAMETER_VALUE));

    private static final URI HTTP_LOCALHOST_QUERY_EMPTY_VALUE_URI = URI.create(String.format("%s?%s", HTTP_LOCALHOST_RESOURCES_URI, PARAMETER_NAME));

    @Test
    void testBuildIllegalStateException() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder();

        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void testBuildSchemeHost() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_URI, uri);
    }

    @Test
    void testBuildSchemeHostPort() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .port(PORT);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_PORT_URI, uri);
    }

    @Test
    void testBuildSchemeHostPortEncodedPath() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .port(PORT)
                .encodedPath(ENCODED_PATH);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_PORT_ENCODED_PATH_URI, uri);
    }

    @Test
    void testBuildSchemeHostPathSegment() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .addPathSegment(RESOURCES_PATH_SEGMENT);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_RESOURCES_URI, uri);
    }

    @Test
    void testBuildSchemeHostPathSegmentQueryParameter() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .addPathSegment(RESOURCES_PATH_SEGMENT)
                .addQueryParameter(PARAMETER_NAME, PARAMETER_VALUE);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_QUERY_URI, uri);
    }

    @Test
    void testBuildSchemeHostPathSegmentQueryParameterNullValue() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .addPathSegment(RESOURCES_PATH_SEGMENT)
                .addQueryParameter(PARAMETER_NAME, null);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_QUERY_EMPTY_VALUE_URI, uri);
    }
}
