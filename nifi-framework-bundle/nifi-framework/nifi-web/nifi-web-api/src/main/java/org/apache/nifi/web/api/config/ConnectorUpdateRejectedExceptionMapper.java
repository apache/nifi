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

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.nifi.components.connector.ConnectorUpdateRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps {@link ConnectorUpdateRejectedException} into client responses with HTTP 409 Conflict status.
 * This exception is thrown when a connector update is rejected by the {@link org.apache.nifi.components.connector.ConnectorRepository},
 * typically due to external modifications or concurrent access conflicts.
 */
@Provider
public class ConnectorUpdateRejectedExceptionMapper implements ExceptionMapper<ConnectorUpdateRejectedException> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorUpdateRejectedExceptionMapper.class);

    @Override
    public Response toResponse(final ConnectorUpdateRejectedException exception) {
        logger.warn("Connector update rejected for connector {}: {}. Returning {} response.",
                exception.getConnectorId(), exception.getMessage(), Response.Status.CONFLICT);
        return Response.status(Response.Status.CONFLICT).entity(exception.getMessage()).type("text/plain").build();
    }
}
