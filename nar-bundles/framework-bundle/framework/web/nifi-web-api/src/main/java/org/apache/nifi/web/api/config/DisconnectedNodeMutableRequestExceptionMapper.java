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
import org.apache.nifi.cluster.manager.exception.DisconnectedNodeMutableRequestException;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps disconnected node mutable request exceptions into client responses.
 */
@Provider
public class DisconnectedNodeMutableRequestExceptionMapper implements ExceptionMapper<DisconnectedNodeMutableRequestException> {

    private static final Logger logger = LoggerFactory.getLogger(DisconnectedNodeMutableRequestExceptionMapper.class);

    @Override
    public Response toResponse(DisconnectedNodeMutableRequestException ex) {
        // log the error
        logger.info(String.format("Unable to service mutable request because a node is disconnected from the cluster: %s. Returning %s response.", ex, Response.Status.CONFLICT));

        if (logger.isDebugEnabled()) {
            logger.debug(StringUtils.EMPTY, ex);
        }

        return Response.status(Response.Status.CONFLICT).entity(
                "Unable to service request because at least one node is disconnected from the cluster.  "
                + "The flow may be changed when all are connected.").type("text/plain").build();
    }

}
