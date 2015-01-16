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
import org.apache.nifi.cluster.manager.exception.IneligiblePrimaryNodeException;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps no ineligible primary node exceptions into client responses.
 */
@Provider
public class IneligiblePrimaryNodeExceptionMapper implements ExceptionMapper<IneligiblePrimaryNodeException> {

    private static final Logger logger = LoggerFactory.getLogger(IneligiblePrimaryNodeException.class);

    @Override
    public Response toResponse(IneligiblePrimaryNodeException ex) {
        // log the error
        logger.info(String.format("Ineligible node requested to be the primary due to %s. Returning %s response.", ex, Response.Status.CONFLICT));

        if (logger.isDebugEnabled()) {
            logger.debug(StringUtils.EMPTY, ex);
        }

        return Response.status(Response.Status.CONFLICT).entity("Node is ineligible to be the primary node.").type("text/plain").build();
    }

}
