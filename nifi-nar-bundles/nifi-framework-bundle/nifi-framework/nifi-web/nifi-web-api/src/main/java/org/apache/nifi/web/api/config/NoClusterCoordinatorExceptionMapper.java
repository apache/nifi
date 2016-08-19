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

import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoClusterCoordinatorExceptionMapper implements ExceptionMapper<NoClusterCoordinatorException> {
    private static final Logger logger = LoggerFactory.getLogger(NoConnectedNodesException.class);

    @Override
    public Response toResponse(final NoClusterCoordinatorException ex) {
        // log the error
        logger.info(String.format("Cluster failed processing request: %s. Returning %s response.", ex, Response.Status.SERVICE_UNAVAILABLE));

        if (logger.isDebugEnabled()) {
            logger.debug(StringUtils.EMPTY, ex);
        }

        return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(ex.getMessage()).type("text/plain").build();
    }

}
