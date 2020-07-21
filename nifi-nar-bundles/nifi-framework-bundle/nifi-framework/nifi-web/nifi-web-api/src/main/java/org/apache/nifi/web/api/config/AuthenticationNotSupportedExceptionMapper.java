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
import org.apache.nifi.authentication.exception.AuthenticationNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps web application exceptions into client responses.
 */
@Provider
public class AuthenticationNotSupportedExceptionMapper implements ExceptionMapper<AuthenticationNotSupportedException> {

    private static final Logger logger = LoggerFactory.getLogger(AuthenticationNotSupportedExceptionMapper.class);

    /**
     * Returns a {@link Response} to the client which translates the provided
     * exception. Ideally, these requests would not be made when HTTPS is off, but
     * these expected messages confuse users reading the logs.
     *
     * @param exception the exception indicating HTTPS is disabled
     * @return the response to the client
     */
    @Override
    public Response toResponse(AuthenticationNotSupportedException exception) {
        // Use DEBUG level to avoid polluting the logs
        logger.debug("{}. Returning {} response.", exception, Response.Status.CONFLICT, exception);
        return Response.status(Response.Status.CONFLICT).entity(exception.getMessage()).type("text/plain").build();
    }

}
