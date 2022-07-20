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

import java.net.URI;

/**
 * Web Client Service Exception provides a generalized wrapper for HTTP communication failures
 */
public class WebClientServiceException extends RuntimeException {
    /**
     * Web Service Client Exception with standard HTTP request properties
     *
     * @param message Failure message
     * @param cause Failure cause
     * @param uri HTTP Request URI
     * @param httpRequestMethod HTTP Request Method
     */
    public WebClientServiceException(
            final String message,
            final Throwable cause,
            final URI uri,
            final HttpRequestMethod httpRequestMethod
    ) {
        super(getMessage(message, uri, httpRequestMethod), cause);
    }

    private static String getMessage(final String message, final URI uri, final HttpRequestMethod httpRequestMethod) {
        return String.format("%s HTTP Method [%s] URI [%s]", message, httpRequestMethod, uri);
    }
}
