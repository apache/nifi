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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Component
@Provider
public class ConstraintViolationExceptionMapper implements ExceptionMapper<ConstraintViolationException> {

    private static final Logger logger = LoggerFactory.getLogger(ConstraintViolationExceptionMapper.class);

    @Override
    public Response toResponse(ConstraintViolationException exception) {
        // start with the overall message which will be something like "Cannot create xyz"
        final StringBuilder errorMessage = new StringBuilder(exception.getMessage()).append(" - ");

        boolean first = true;
        for (final ConstraintViolation violation : exception.getConstraintViolations()) {
            if (!first) {
                errorMessage.append(", ");
            }
            first = false;

            // lastNode should end up as the field that failed validation
            Path.Node lastNode = null;
            for (final Path.Node node : violation.getPropertyPath()) {
                lastNode = node;
            }

            // append something like "xyz must not be..."
            errorMessage.append(lastNode.getName()).append(" ").append(violation.getMessage());
        }

        logger.info(String.format("%s. Returning %s response.", errorMessage, Response.Status.BAD_REQUEST));
        if (logger.isDebugEnabled()) {
            logger.debug(StringUtils.EMPTY, exception);
        }

        return Response.status(Response.Status.BAD_REQUEST).entity(errorMessage.toString()).type("text/plain").build();
    }

}
