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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps access denied exceptions into a client response.
 */
@Provider
public class AccessDeniedExceptionMapper implements ExceptionMapper<AccessDeniedException> {

    private static final Logger logger = LoggerFactory.getLogger(AccessDeniedExceptionMapper.class);

    @Override
    public Response toResponse(AccessDeniedException exception) {
        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // if the user was authenticated - forbidden, otherwise unauthorized... the user may be null if the
        // AccessDeniedException was thrown from a /access endpoint that isn't subject to the security
        // filter chain. for instance, one that performs kerberos negotiation
        final Response.Status status;
        if (user == null || user.isAnonymous()) {
            status = Status.UNAUTHORIZED;
        } else {
            status = Status.FORBIDDEN;
        }

        final String identity;
        if (user == null) {
            identity = "<no user found>";
        } else {
            identity = user.toString();
        }

        logger.info(String.format("%s does not have permission to access the requested resource. %s Returning %s response.", identity, exception.getMessage(), status));

        if (logger.isDebugEnabled()) {
            logger.debug(StringUtils.EMPTY, exception);
        }

        return Response.status(status)
                .entity(String.format("%s Contact the system administrator.", exception.getMessage()))
                .type("text/plain")
                .build();
    }

}
