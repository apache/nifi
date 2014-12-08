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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps web application exceptions into client responses.
 */
@Provider
public class WebApplicationExceptionMapper implements ExceptionMapper<WebApplicationException> {

    private static final Logger logger = LoggerFactory.getLogger(WebApplicationExceptionMapper.class);
    private static final String EXCEPTION_SEPARATOR = ": ";

    @Override
    public Response toResponse(WebApplicationException exception) {
        // get the message and ensure it is not blank
        String message = exception.getMessage();
        if (message == null) {
            message = StringUtils.EMPTY;
        }

        // format the message
        if (message.contains(EXCEPTION_SEPARATOR)) {
            message = StringUtils.substringAfter(message, EXCEPTION_SEPARATOR);
        }

        // get the response
        final Response response = exception.getResponse();

        // log the error
        logger.info(String.format("%s. Returning %s response.", exception, response.getStatus()));

        if (logger.isDebugEnabled()) {
            logger.debug(StringUtils.EMPTY, exception);
        }

        // generate the response
        return Response.status(response.getStatus()).entity(message).type("text/plain").build();
    }

}
