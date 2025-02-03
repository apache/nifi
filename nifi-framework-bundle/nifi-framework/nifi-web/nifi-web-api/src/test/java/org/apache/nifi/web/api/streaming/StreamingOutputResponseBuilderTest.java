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
package org.apache.nifi.web.api.streaming;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamingOutputResponseBuilderTest {

    private static final byte[] INPUT_BYTES = String.class.getSimpleName().getBytes(StandardCharsets.UTF_8);

    private static final String RANGE = "bytes=0-%d".formatted(INPUT_BYTES.length);

    private static final String ACCEPT_RANGES_BYTES = "bytes";

    private static final String CONTENT_RANGE_EXPECTED = "bytes 0-%d/%d".formatted(INPUT_BYTES.length, INPUT_BYTES.length);

    @Test
    void testBuildInputStream() {
        final InputStream inputStream = new ByteArrayInputStream(INPUT_BYTES);

        final StreamingOutputResponseBuilder builder = new StreamingOutputResponseBuilder(inputStream);

        final Response.ResponseBuilder responseBuilder = builder.build();

        try (Response response = responseBuilder.build()) {
            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        }
    }

    @Test
    void testBuildRange() {
        final InputStream inputStream = new ByteArrayInputStream(INPUT_BYTES);

        final StreamingOutputResponseBuilder builder = new StreamingOutputResponseBuilder(inputStream);
        builder.range(RANGE);

        final Response.ResponseBuilder responseBuilder = builder.build();

        try (Response response = responseBuilder.build()) {
            assertEquals(Response.Status.PARTIAL_CONTENT.getStatusCode(), response.getStatus());

            final String acceptRanges = response.getHeaderString(StreamingOutputResponseBuilder.ACCEPT_RANGES_HEADER);
            assertEquals(ACCEPT_RANGES_BYTES, acceptRanges);

            final String contentRange = response.getHeaderString(StreamingOutputResponseBuilder.CONTENT_RANGE_HEADER);
            assertEquals(CONTENT_RANGE_EXPECTED, contentRange);
        }
    }
}
