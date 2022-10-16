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

import java.io.IOException;
import java.util.stream.Stream;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;

public enum C2RequestCompression {
    NONE("none") {
        @Override
        public Request compress(Request request) {
            return request;
        }
    },
    GZIP("gzip") {
        @Override
        public Request compress(Request request) {
            return request.newBuilder()
                .header(CONTENT_ENCODING_HEADER, GZIP_ENCODING)
                .method(request.method(), toGzipRequestBody(request.body()))
                .build();
        }

        private RequestBody toGzipRequestBody(RequestBody requestBody) {
            return new RequestBody() {
                @Override
                public MediaType contentType() {
                    return requestBody.contentType();
                }

                @Override
                public long contentLength() {
                    return -1;
                }

                @Override
                public void writeTo(BufferedSink sink) throws IOException {
                    try (BufferedSink bufferedGzipSink = Okio.buffer(new GzipSink(sink))) {
                        requestBody.writeTo(bufferedGzipSink);
                    }
                }
            };
        }
    };

    static final String CONTENT_ENCODING_HEADER = "Content-Encoding";
    static final String GZIP_ENCODING = "gzip";

    private final String compressionType;

    C2RequestCompression(String compressionType) {
        this.compressionType = compressionType;
    }

    public static C2RequestCompression forType(String compressionType) {
        return Stream.of(values())
            .filter(c2RequestCompression -> c2RequestCompression.compressionType.equalsIgnoreCase(compressionType))
            .findAny()
            .orElse(NONE);
    }

    public abstract Request compress(Request request);
}