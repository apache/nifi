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

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps unknown node exceptions into client responses.
 */
@Provider
public class ThrowableMapper implements ExceptionMapper<Throwable> {

    private static final Logger logger = LoggerFactory.getLogger(ThrowableMapper.class);

    @Override
    public Response toResponse(Throwable exception) {
        // log the error
        logger.error(String.format("An unexpected error has occurred: %s. Returning %s response.", exception, Response.Status.INTERNAL_SERVER_ERROR), exception);

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("An unexpected error has occurred. Please check the logs for additional details.").type("text/plain").build();
    }

}
