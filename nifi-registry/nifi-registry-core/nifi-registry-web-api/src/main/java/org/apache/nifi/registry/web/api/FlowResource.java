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
package org.apache.nifi.registry.web.api;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.Extension;
import io.swagger.annotations.ExtensionProperty;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Set;
import java.util.SortedSet;

@Component
@Path("/flows")
@Api(
        value = "flows",
        description = "Gets metadata about flows.",
        authorizations = { @Authorization("Authorization") }
)
public class FlowResource extends ApplicationResource {

    @Autowired
    public FlowResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @GET
    @Path("fields")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get flow fields",
            notes = "Retrieves the flow field names that can be used for searching or sorting on flows.",
            response = Fields.class
    )
    public Response getAvailableFlowFields() {
        final Set<String> flowFields = serviceFacade.getFlowFields();
        final Fields fields = new Fields(flowFields);
        return Response.status(Response.Status.OK).entity(fields).build();
    }

    @GET
    @Path("{flowId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get flow",
            notes = "Gets a flow by id.",
            nickname = "globalGetFlow",
            response = VersionedFlow.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response getFlow(
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final VersionedFlow flow = serviceFacade.getFlow(flowId);
        return Response.status(Response.Status.OK).entity(flow).build();
    }

    @GET
    @Path("{flowId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get flow versions",
            notes = "Gets summary information for all versions of a given flow. Versions are ordered newest->oldest.",
            nickname = "globalGetFlowVersions",
            response = VersionedFlowSnapshotMetadata.class,
            responseContainer = "List",
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response getFlowVersions(
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final SortedSet<VersionedFlowSnapshotMetadata> snapshots = serviceFacade.getFlowSnapshots(flowId);
        return Response.status(Response.Status.OK).entity(snapshots).build();
    }

    @GET
    @Path("{flowId}/versions/{versionNumber: \\d+}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get flow version",
            notes = "Gets the given version of a flow, including metadata and flow content.",
            nickname = "globalGetFlowVersion",
            response = VersionedFlowSnapshot.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response getFlowVersion(
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId,
            @PathParam("versionNumber")
            @ApiParam("The version number")
                final Integer versionNumber) {

        final VersionedFlowSnapshot snapshot = serviceFacade.getFlowSnapshot(flowId, versionNumber);
        return Response.status(Response.Status.OK).entity(snapshot).build();
    }

    @GET
    @Path("{flowId}/versions/latest")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get latest flow version",
            notes = "Gets the latest version of a flow, including metadata and flow content.",
            nickname = "globalGetLatestFlowVersion",
            response = VersionedFlowSnapshot.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response getLatestFlowVersion(
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final VersionedFlowSnapshot lastSnapshot = serviceFacade.getLatestFlowSnapshot(flowId);
        return Response.status(Response.Status.OK).entity(lastSnapshot).build();
    }

    @GET
    @Path("{flowId}/versions/latest/metadata")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get latest flow version metadata",
            notes = "Gets the metadata for the latest version of a flow.",
            nickname = "globalGetLatestFlowVersionMetadata",
            response = VersionedFlowSnapshotMetadata.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response getLatestFlowVersionMetadata(
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final VersionedFlowSnapshotMetadata latestMetadata = serviceFacade.getLatestFlowSnapshotMetadata(flowId);
        return Response.status(Response.Status.OK).entity(latestMetadata).build();
    }

}
