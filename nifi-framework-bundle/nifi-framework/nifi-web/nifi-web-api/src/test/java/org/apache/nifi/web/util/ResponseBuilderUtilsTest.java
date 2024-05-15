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


import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResponseBuilderUtilsTest {

    private static final String FILENAME_ASCII = "image.jpg";

    private static final String DISPOSITION_ASCII = "attachment; filename=\"=?UTF-8?Q?%s?=\"; filename*=UTF-8''%s".formatted(FILENAME_ASCII, FILENAME_ASCII);

    private static final String FILENAME_SPACED = "image label.jpg";

    private static final String DISPOSITION_ENCODED = "attachment; filename=\"=?UTF-8?Q?image_label.jpg?=\"; filename*=UTF-8''image%20label.jpg";

    @Test
    void testSetContentDisposition() {
        final Response.ResponseBuilder responseBuilder = ResponseBuilderUtils.setContentDisposition(Response.ok(), FILENAME_ASCII);

        try (Response response = responseBuilder.build()) {
            final String contentDisposition = response.getHeaderString(HttpHeaders.CONTENT_DISPOSITION);

            assertEquals(DISPOSITION_ASCII, contentDisposition);
        }
    }

    @Test
    void testSetContentDispositionEncoded() {
        final Response.ResponseBuilder responseBuilder = ResponseBuilderUtils.setContentDisposition(Response.ok(), FILENAME_SPACED);

        try (Response response = responseBuilder.build()) {
            final String contentDisposition = response.getHeaderString(HttpHeaders.CONTENT_DISPOSITION);

            assertEquals(DISPOSITION_ENCODED, contentDisposition);
        }
    }
}
