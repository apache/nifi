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
package org.apache.nifi.web.api;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.entity.SystemDiagnosticsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for retrieving system diagnostics.
 */
@Path("/system-diagnostics")
public class SystemDiagnosticsResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(SystemDiagnosticsResource.class);

    private NiFiServiceFacade serviceFacade;

    /**
     * Gets the system diagnostics for this NiFi instance.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A systemDiagnosticsEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(SystemDiagnosticsEntity.class)
    public Response getSystemDiagnostics(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {
        final SystemDiagnosticsDTO systemDiagnosticsDto = serviceFacade.getSystemDiagnostics();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response
        final SystemDiagnosticsEntity entity = new SystemDiagnosticsEntity();
        entity.setRevision(revision);
        entity.setSystemDiagnostics(systemDiagnosticsDto);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }
}
