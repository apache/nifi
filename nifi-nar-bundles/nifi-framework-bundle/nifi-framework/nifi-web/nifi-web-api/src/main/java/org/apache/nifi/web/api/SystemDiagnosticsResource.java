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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.entity.SystemDiagnosticsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.springframework.security.access.prepost.PreAuthorize;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.Set;

/**
 * RESTful endpoint for retrieving system diagnostics.
 */
@Path("/system-diagnostics")
@Api(
        value = "/system-diagnostics",
        description = "Provides diagnostics for the system NiFi is running on"
)
public class SystemDiagnosticsResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;


    /**
     * Gets the system diagnostics for this NiFi instance.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A systemDiagnosticsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the diagnostics for the system NiFi is running on",
            response = SystemDiagnosticsEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),}
    )
    public Response getSystemDiagnostics(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                value = "Whether or not to include the breakdown per node. Optional, defaults to false",
                required = false
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @ApiParam(
                value = "The id of the node where to get the status.",
                required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId) {

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final SystemDiagnosticsEntity entity = (SystemDiagnosticsEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getSystemDiagnostics().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
            }
        }

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

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
