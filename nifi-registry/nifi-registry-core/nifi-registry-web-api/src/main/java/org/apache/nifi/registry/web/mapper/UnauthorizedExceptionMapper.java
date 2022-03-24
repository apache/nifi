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
package org.apache.nifi.registry.web.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.web.exception.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps Unauthorized exceptions into client responses that set the WWW-Authenticate header
 * with a list of challenges (i.e., acceptable auth scheme types).
 */
@Component
@Provider
public class UnauthorizedExceptionMapper implements ExceptionMapper<UnauthorizedException> {

    private static final Logger logger = LoggerFactory.getLogger(UnauthorizedExceptionMapper.class);

    private static final String AUTHENTICATION_CHALLENGE_HEADER_NAME = "WWW-Authenticate";

    @Override
    public Response toResponse(UnauthorizedException exception) {

        logger.info("{}. Returning {} response.", exception, Response.Status.UNAUTHORIZED);
        logger.debug(StringUtils.EMPTY, exception);

        final Response.ResponseBuilder response = Response.status(Response.Status.UNAUTHORIZED);
        if (exception.getWwwAuthenticateChallenge() != null) {
            response.header(AUTHENTICATION_CHALLENGE_HEADER_NAME, exception.getWwwAuthenticateChallenge());
        }
        response.entity(exception.getMessage()).type("text/plain");
        return response.build();

    }

}
