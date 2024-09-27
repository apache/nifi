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

    private static final String ENCODED_PATH_WITH_TRAILING_SEPARATOR = "/resources/search/";

    private static final String PATH_WITH_SPACES_ENCODED = "/resources/%20separated%20search";

    private static final String BUCKETS_PATH_SEGMENT = "buckets";

    private static final String FILES_PATH_SEGMENT = "files";

    private static final String RESOURCES_PATH_SEGMENT = "resources";

    private static final String RESOURCES_PATH_SEGMENT_SEPARATED = "resources|separated";

    private static final String RESOURCES_PATH_SEGMENT_SEPARATED_ENCODED = "resources%7Cseparated";

    private static final String PARAMETER_NAME = "search";

    private static final String PARAMETER_VALUE = "terms";

    private static final String SECOND_PARAMETER_NAME = "refresh";

    private static final String SECOND_PARAMETER_VALUE = "{0}";

    private static final String SECOND_PARAMETER_VALUE_ENCODED = "%7B0%7D";

    private static final String PARAMETER_NAME_AND_VALUE = String.format("%s=%s", PARAMETER_NAME, PARAMETER_VALUE);

    private static final URI HTTP_LOCALHOST_URI = URI.create(
            String.format("%s://%s/", HTTP_SCHEME, LOCALHOST)
    );

    private static final URI HTTP_LOCALHOST_PORT_URI = URI.create(
            String.format("%s://%s:%d/", HTTP_SCHEME, LOCALHOST, PORT)
    );

    private static final URI HTTP_LOCALHOST_PORT_ENCODED_PATH_URI = URI.create(
            String.format("%s://%s:%d%s", HTTP_SCHEME, LOCALHOST, PORT, ENCODED_PATH)
    );

    private static final URI HTTP_LOCALHOST_PORT_ENCODED_PATH_WITH_SPACES_URI = URI.create(
            String.format("%s://%s:%d%s", HTTP_SCHEME, LOCALHOST, PORT, PATH_WITH_SPACES_ENCODED)
    );

    private static final URI HTTP_LOCALHOST_PORT_ENCODED_PATH_WITH_SPACES_AND_SEGMENTS_URI = URI.create(
            String.format("%s://%s:%d%s/%s/%s", HTTP_SCHEME, LOCALHOST, PORT, PATH_WITH_SPACES_ENCODED, BUCKETS_PATH_SEGMENT, FILES_PATH_SEGMENT)
    );

    private static final URI HTTP_LOCALHOST_PORT_ENCODED_PATH_WITH_TRAILING_SEPARATOR_AND_SEGMENTS_URI = URI.create(
            String.format("%s://%s:%d%s%s/%s", HTTP_SCHEME, LOCALHOST, PORT, ENCODED_PATH_WITH_TRAILING_SEPARATOR, BUCKETS_PATH_SEGMENT, FILES_PATH_SEGMENT)
    );

    private static final URI HTTP_LOCALHOST_RESOURCES_URI = URI.create(
            String.format("%s%s", HTTP_LOCALHOST_URI, RESOURCES_PATH_SEGMENT)
    );

    private static final URI HTTP_LOCALHOST_RESOURCES_SEPARATED_URI = URI.create(
            String.format("%s%s", HTTP_LOCALHOST_URI, RESOURCES_PATH_SEGMENT_SEPARATED_ENCODED)
    );

    private static final URI HTTP_LOCALHOST_QUERY_URI = URI.create(
            String.format("%s?%s", HTTP_LOCALHOST_RESOURCES_URI, PARAMETER_NAME_AND_VALUE)
    );

    private static final URI HTTP_LOCALHOST_QUERY_REPEATED_URI = URI.create(
            String.format("%s?%s&%s", HTTP_LOCALHOST_RESOURCES_URI, PARAMETER_NAME_AND_VALUE, PARAMETER_NAME_AND_VALUE)
    );

    private static final URI HTTP_LOCALHOST_QUERY_SECOND_PARAMETER_URI = URI.create(
            String.format("%s?%s&%s", HTTP_LOCALHOST_RESOURCES_URI, PARAMETER_NAME_AND_VALUE, SECOND_PARAMETER_NAME)
    );

    private static final URI HTTP_LOCALHOST_QUERY_PARAMETER_VALUE_URI = URI.create(
            String.format("%s?%s=%s", HTTP_LOCALHOST_RESOURCES_URI, SECOND_PARAMETER_NAME, SECOND_PARAMETER_VALUE_ENCODED)
    );

    private static final URI HTTP_LOCALHOST_QUERY_EMPTY_VALUE_URI = URI.create(
            String.format("%s?%s", HTTP_LOCALHOST_RESOURCES_URI, PARAMETER_NAME)
    );

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
    void testBuildSchemeInvalid() {
        assertThrows(IllegalArgumentException.class, () -> new StandardHttpUriBuilder().scheme(String.class.getSimpleName()));
    }

    @Test
    void testBuildPortInvalidMaximum() {
        assertThrows(IllegalArgumentException.class, () -> new StandardHttpUriBuilder().port(Integer.MAX_VALUE));
    }

    @Test
    void testBuildPortInvalidMinimum() {
        assertThrows(IllegalArgumentException.class, () -> new StandardHttpUriBuilder().port(Integer.MIN_VALUE));
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
    void testBuildSchemeHostPortEncodedPathWithSpaces() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .port(PORT)
                .encodedPath(PATH_WITH_SPACES_ENCODED);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_PORT_ENCODED_PATH_WITH_SPACES_URI, uri);
    }

    @Test
    void testBuildSchemeHostPortEncodedPathWithSpacesAndPathSegments() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .port(PORT)
                .encodedPath(PATH_WITH_SPACES_ENCODED)
                .addPathSegment(BUCKETS_PATH_SEGMENT)
                .addPathSegment(FILES_PATH_SEGMENT);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_PORT_ENCODED_PATH_WITH_SPACES_AND_SEGMENTS_URI, uri);
    }

    @Test
    void testBuildSchemeHostPortEncodedPathWithTrailingSeparatorAndPathSegments() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .port(PORT)
                .encodedPath(ENCODED_PATH_WITH_TRAILING_SEPARATOR)
                .addPathSegment(BUCKETS_PATH_SEGMENT)
                .addPathSegment(FILES_PATH_SEGMENT);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_PORT_ENCODED_PATH_WITH_TRAILING_SEPARATOR_AND_SEGMENTS_URI, uri);
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
    void testBuildSchemeHostPathSegmentSeparated() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .addPathSegment(RESOURCES_PATH_SEGMENT_SEPARATED);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_RESOURCES_SEPARATED_URI, uri);
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
    void testBuildSchemeHostPathSegmentQueryParameterRepeated() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .addPathSegment(RESOURCES_PATH_SEGMENT)
                .addQueryParameter(PARAMETER_NAME, PARAMETER_VALUE)
                .addQueryParameter(PARAMETER_NAME, PARAMETER_VALUE);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_QUERY_REPEATED_URI, uri);
    }

    @Test
    void testBuildSchemeHostPathSegmentSecondQueryParameter() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .addPathSegment(RESOURCES_PATH_SEGMENT)
                .addQueryParameter(PARAMETER_NAME, PARAMETER_VALUE)
                .addQueryParameter(SECOND_PARAMETER_NAME, null);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_QUERY_SECOND_PARAMETER_URI, uri);
    }

    @Test
    void testBuildSchemeHostPathSegmentQueryParameterValueEncoded() {
        final HttpUriBuilder builder = new StandardHttpUriBuilder()
                .scheme(HTTP_SCHEME)
                .host(LOCALHOST)
                .addPathSegment(RESOURCES_PATH_SEGMENT)
                .addQueryParameter(SECOND_PARAMETER_NAME, SECOND_PARAMETER_VALUE);

        final URI uri = builder.build();

        assertEquals(HTTP_LOCALHOST_QUERY_PARAMETER_VALUE_URI, uri);
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
