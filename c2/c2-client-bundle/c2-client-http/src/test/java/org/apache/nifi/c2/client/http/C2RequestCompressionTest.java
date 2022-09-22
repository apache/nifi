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

package org.apache.nifi.c2.client.http;

import static org.apache.nifi.c2.client.http.C2HttpClient.MEDIA_TYPE_APPLICATION_JSON;
import static org.apache.nifi.c2.client.http.C2RequestCompression.CONTENT_ENCODING_HEADER;
import static org.apache.nifi.c2.client.http.C2RequestCompression.GZIP;
import static org.apache.nifi.c2.client.http.C2RequestCompression.GZIP_ENCODING;
import static org.apache.nifi.c2.client.http.C2RequestCompression.NONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.stream.Stream;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.Buffer;
import okio.GzipSource;
import okio.Okio;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class C2RequestCompressionTest {

    private static final String DEFAULT_C2_SERVER_URL = "http://localhost/c2";
    private static final String DEFAULT_POST_BODY = "{ \"field\": \"value\" }";

    private static Stream<Arguments> compressionTypes() {
        return Stream.of(
            Arguments.of("none", NONE),
            Arguments.of("gzip", GZIP),
            Arguments.of("unknown_compression_type", NONE)
        );
    }

    @ParameterizedTest
    @MethodSource("compressionTypes")
    public void testAppropriateCompressionTypeIsGivenBackForType(String compressionType, C2RequestCompression expectedCompression) {
        assertEquals(expectedCompression, C2RequestCompression.forType(compressionType));
    }

    @Test
    public void testNoneCompressionShouldLeaveRequestBodyIntact() throws IOException {
        // given
        Request request = new Request.Builder()
            .post(RequestBody.create(DEFAULT_POST_BODY, MEDIA_TYPE_APPLICATION_JSON))
            .url(DEFAULT_C2_SERVER_URL)
            .build();

        // when
        Request result = NONE.compress(request);

        // then
        assertTrue(result.body().contentType().toString().contains(MEDIA_TYPE_APPLICATION_JSON.toString()));
        assertEquals(DEFAULT_POST_BODY, uncompressedRequestBodyToString(result));
    }

    @Test
    public void testGzipCompressionShouldCompressRequestBodyAndAdjustRequestHeader() throws IOException {
        // given
        Request request = new Request.Builder()
            .post(RequestBody.create(DEFAULT_POST_BODY, MEDIA_TYPE_APPLICATION_JSON))
            .url(DEFAULT_C2_SERVER_URL)
            .build();

        // when
        Request result = GZIP.compress(request);

        // then
        assertTrue(result.body().contentType().toString().contains(MEDIA_TYPE_APPLICATION_JSON.toString()));
        assertEquals(GZIP_ENCODING, result.headers().get(CONTENT_ENCODING_HEADER));
        assertEquals(DEFAULT_POST_BODY, gzippedRequestBodyToString(result));
    }

    private String uncompressedRequestBodyToString(Request request) throws IOException {
        Buffer buffer = requestToBuffer(request);
        return buffer.readUtf8();
    }

    private String gzippedRequestBodyToString(Request request) throws IOException {
        Buffer buffer = requestToBuffer(request);
        return Okio.buffer(new GzipSource(buffer)).readUtf8();
    }

    private Buffer requestToBuffer(Request request) throws IOException {
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        return buffer;
    }
}
