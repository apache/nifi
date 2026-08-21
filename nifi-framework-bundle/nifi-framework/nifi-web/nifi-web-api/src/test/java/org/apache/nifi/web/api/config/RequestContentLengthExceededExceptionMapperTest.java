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
package org.apache.nifi.web.api.config;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.web.security.requests.RequestContentLengthExceededException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestContentLengthExceededExceptionMapperTest {

    private static final String MESSAGE = "Content Too Large";

    private final RequestContentLengthExceededExceptionMapper mapper = new RequestContentLengthExceededExceptionMapper();

    @Test
    void testToResponseRequestEntityTooLarge() {
        final RequestContentLengthExceededException exception = new RequestContentLengthExceededException(MESSAGE);

        try (Response response = mapper.toResponse(exception)) {
            assertEquals(Response.Status.REQUEST_ENTITY_TOO_LARGE.getStatusCode(), response.getStatus());
            assertEquals(MESSAGE, response.getEntity());
            assertEquals(MediaType.TEXT_PLAIN_TYPE, response.getMediaType());
        }
    }
}
