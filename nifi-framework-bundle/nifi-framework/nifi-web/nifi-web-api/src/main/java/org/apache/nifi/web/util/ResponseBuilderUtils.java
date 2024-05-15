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
package org.apache.nifi.web.util;

import jakarta.ws.rs.core.Response;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;

import java.nio.charset.StandardCharsets;

/**
 * HTTP Response Builder Utilities
 */
public class ResponseBuilderUtils {
    /**
     * Set Content-Disposition Header with filename encoded according to RFC requirements
     *
     * @param responseBuilder HTTP Response Builder
     * @param filename Filename to be encoded for Content-Disposition header
     * @return HTTP Response Builder
     */
    public static Response.ResponseBuilder setContentDisposition(final Response.ResponseBuilder responseBuilder, final String filename) {
        final String disposition = ContentDisposition.attachment()
                .filename(filename, StandardCharsets.UTF_8)
                .build()
                .toString();

        return responseBuilder.header(HttpHeaders.CONTENT_DISPOSITION, disposition);
    }
}
