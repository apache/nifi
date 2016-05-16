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

import com.sun.jersey.api.core.ResourceContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.search.NodeSearchResultDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ClusterSearchResultsEntity;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

/**
 * RESTful endpoint for managing a cluster.
 */
@Path("/cluster")
@Api(
        value = "/cluster",
        description = "Endpoint for managing the cluster."
)
public class ClusterResource extends ApplicationResource {

    @Context
    private ResourceContext resourceContext;
    private NiFiServiceFacade serviceFacade;
    private NiFiProperties properties;

    /**
     * Locates the ClusterConnection sub-resource.
     *
     * @return node resource
     */
    @Path("/nodes")
    @ApiOperation(
            value = "Gets the node resource",
            response = NodeResource.class
    )
    public NodeResource getNodeResource() {
        return resourceContext.getResource(NodeResource.class);
    }

    /**
     * Returns a 200 OK response to indicate this is a valid cluster endpoint.
     *
     * @return An OK response with an empty entity body.
     */
    @HEAD
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response getClusterHead() {
        if (properties.isClusterManager()) {
            return Response.ok().build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).entity("Only a cluster manager can process the request.").build();
        }
    }

    /**
     * Gets the contents of this NiFi cluster. This includes all nodes and their status.
     *
     * @return A clusterEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the contents of the cluster",
            notes = "Returns the contents of the cluster including all nodes and their status.",
            response = ClusterEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "DFM", type = "ROLE_DFM"),
                @Authorization(value = "Admin", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getCluster() {

        if (properties.isClusterManager()) {

            final ClusterDTO dto = serviceFacade.getCluster();

            // create entity
            final ClusterEntity entity = new ClusterEntity();
            entity.setCluster(dto);

            // generate the response
            return generateOkResponse(entity).build();
        }

        throw new IllegalClusterResourceRequestException("Only a cluster manager can process the request.");
    }

    /**
     * Searches the cluster for a node with a given address.
     *
     * @param value Search value that will be matched against a node's address
     * @return Nodes that match the specified criteria
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Path("/search-results")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Searches the cluster for a node with the specified address",
            response = ClusterSearchResultsEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "DFM", type = "ROLE_DFM"),
                @Authorization(value = "Admin", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 404, message = "The specified resource could not be found."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response searchCluster(
            @ApiParam(
                    value = "Node address to search for.",
                    required = true
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {

        // ensure this is the cluster manager
        if (properties.isClusterManager()) {
            final List<NodeSearchResultDTO> nodeMatches = new ArrayList<>();

            // get the nodes in the cluster
            final ClusterDTO cluster = serviceFacade.getCluster();

            // check each to see if it matches the search term
            for (NodeDTO node : cluster.getNodes()) {
                // ensure the node is connected
                if (!Node.Status.CONNECTED.toString().equals(node.getStatus())) {
                    continue;
                }

                // determine the current nodes address
                final String address = node.getAddress() + ":" + node.getApiPort();

                // count the node if there is no search or it matches the address
                if (StringUtils.isBlank(value) || StringUtils.containsIgnoreCase(address, value)) {
                    final NodeSearchResultDTO nodeMatch = new NodeSearchResultDTO();
                    nodeMatch.setId(node.getNodeId());
                    nodeMatch.setAddress(address);
                    nodeMatches.add(nodeMatch);
                }
            }

            // build the response
            ClusterSearchResultsEntity results = new ClusterSearchResultsEntity();
            results.setNodeResults(nodeMatches);

            // generate an 200 - OK response
            return noCache(Response.ok(results)).build();
        }

        throw new IllegalClusterResourceRequestException("Only a cluster manager can process the request.");
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
