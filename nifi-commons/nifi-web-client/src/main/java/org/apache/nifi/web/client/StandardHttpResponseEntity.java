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

import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpResponseEntity;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Standard implementation of HTTP Response Entity for Standard Web Client Service
 */
class StandardHttpResponseEntity implements HttpResponseEntity {
    private final int statusCode;

    private final HttpEntityHeaders headers;

    private final InputStream body;

    private final URI uri;

    StandardHttpResponseEntity(
            final int statusCode,
            final HttpEntityHeaders headers,
            final InputStream body,
            final URI uri
    ) {
        this.statusCode = statusCode;
        this.headers = headers;
        this.body = body;
        this.uri = uri;
    }

    @Override
    public int statusCode() {
        return statusCode;
    }

    @Override
    public HttpEntityHeaders headers() {
        return headers;
    }

    @Override
    public InputStream body() {
        return body;
    }

    @Override
    public void close() throws IOException {
        body.close();
    }

    @Override
    public URI uri() {
        return uri;
    }
}
