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
import org.apache.nifi.components.connector.ConnectorConfigurationProviderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps connector configuration provider exceptions into client responses. Provider implementations
 * raise this exception for any failure of an external configuration operation (load, save, discard,
 * delete, asset management, etc.). The framework cannot classify the failure further without
 * coupling to a specific provider implementation, so a generic Internal Server Error response is
 * returned with the provider-supplied message in the response body so callers can surface it
 * directly.
 */
@Provider
public class ConnectorConfigurationProviderExceptionMapper implements ExceptionMapper<ConnectorConfigurationProviderException> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorConfigurationProviderExceptionMapper.class);

    @Override
    public Response toResponse(final ConnectorConfigurationProviderException exception) {
        logger.warn("{}. Returning {} response.", exception, Response.Status.INTERNAL_SERVER_ERROR, exception);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).type("text/plain").build();
    }

}
