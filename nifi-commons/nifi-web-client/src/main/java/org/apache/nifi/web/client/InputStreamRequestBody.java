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

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

import java.io.IOException;
import java.io.InputStream;

/**
 * OkHttp Request Body implementation based on an InputStream
 */
class InputStreamRequestBody extends RequestBody {
    private final InputStream inputStream;

    private final long contentLength;

    InputStreamRequestBody(final InputStream inputStream, final long contentLength) {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
    }

    @Override
    public long contentLength() {
        return contentLength;
    }

    @Override
    public MediaType contentType() {
        return null;
    }

    @Override
    public void writeTo(final BufferedSink bufferedSink) throws IOException {
        final Source source = Okio.source(inputStream);
        bufferedSink.writeAll(source);
    }
}
