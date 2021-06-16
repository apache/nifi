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
import java.net.URI;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.web.service.ExportedVersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.revision.web.ClientIdParameter;
import org.apache.nifi.registry.revision.web.LongParameter;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.SortedSet;

@Component
@Path("/buckets/{bucketId}/flows")
@Api(
        value = "bucket flows",
        description = "Create flows scoped to an existing bucket in the registry.",
        authorizations = { @Authorization("Authorization") }
)
public class BucketFlowResource extends ApplicationResource {

    @Autowired
    public BucketFlowResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Create flow",
            notes = "Creates a flow in the given bucket. The flow id is created by the server and populated in the returned entity.",
            response = VersionedFlow.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response createFlow(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @ApiParam(value = "The details of the flow to create.", required = true)
                final VersionedFlow flow) {

        verifyPathParamsMatchBody(bucketId, flow);

        final VersionedFlow createdFlow = serviceFacade.createFlow(bucketId, flow);
        publish(EventFactory.flowCreated(createdFlow));
        return Response.status(Response.Status.OK).entity(createdFlow).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bucket flows",
            notes = "Retrieves all flows in the given bucket.",
            response = VersionedFlow.class,
            responseContainer = "List",
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
    public Response getFlows(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
            final String bucketId) {

        final List<VersionedFlow> flows = serviceFacade.getFlows(bucketId);
        return Response.status(Response.Status.OK).entity(flows).build();
    }

    @GET
    @Path("{flowId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bucket flow",
            notes = "Retrieves the flow with the given id in the given bucket.",
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
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final VersionedFlow flow = serviceFacade.getFlow(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(flow).build();
    }

    @PUT
    @Path("{flowId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update bucket flow",
            notes = "Updates the flow with the given id in the given bucket.",
            response = VersionedFlow.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response updateFlow(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId,
            @ApiParam(value = "The updated flow", required = true)
                final VersionedFlow flow) {

        verifyPathParamsMatchBody(bucketId, flowId, flow);

        // bucketId and flowId fields are optional in the body parameter, but required before calling the service layer
        setBucketItemMetadataIfMissing(bucketId, flowId, flow);

        final VersionedFlow updatedFlow = serviceFacade.updateFlow(flow);
        publish(EventFactory.flowUpdated(updatedFlow));
        return Response.status(Response.Status.OK).entity(updatedFlow).build();
    }

    @DELETE
    @Path("{flowId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Delete bucket flow",
            notes = "Deletes a flow, including all saved versions of that flow.",
            response = VersionedFlow.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "delete"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response deleteFlow(
            @ApiParam(value = "The version is used to verify the client is working with the latest version of the entity.", required = true)
            @QueryParam(VERSION)
                final LongParameter version,
            @ApiParam(value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY)
                final ClientIdParameter clientId,
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final RevisionInfo revisionInfo = getRevisionInfo(version, clientId);
        final VersionedFlow deletedFlow = serviceFacade.deleteFlow(bucketId, flowId, revisionInfo);
        publish(EventFactory.flowDeleted(deletedFlow));

        return Response.status(Response.Status.OK).entity(deletedFlow).build();
    }

    @POST
    @Path("{flowId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Create flow version",
            notes = "Creates the next version of a flow. The version number of the object being created must be the " +
                    "next available version integer. Flow versions are immutable after they are created.",
            response = VersionedFlowSnapshot.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response createFlowVersion(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam(value = "The flow identifier")
                final String flowId,
            @ApiParam(value = "The new versioned flow snapshot.", required = true)
                final VersionedFlowSnapshot snapshot) {

        verifyPathParamsMatchBody(bucketId, flowId, snapshot);

        // bucketId and flowId fields are optional in the body parameter, but required before calling the service layer
        setSnaphotMetadataIfMissing(bucketId, flowId, snapshot);

        final String userIdentity = NiFiUserUtils.getNiFiUserIdentity();
        snapshot.getSnapshotMetadata().setAuthor(userIdentity);

        final VersionedFlowSnapshot createdSnapshot = serviceFacade.createFlowSnapshot(snapshot);
        publish(EventFactory.flowVersionCreated(createdSnapshot));

        return Response.status(Response.Status.OK).entity(createdSnapshot).build();
    }

    @POST
    @Path("{flowId}/versions/import")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Import flow version",
            notes = "Import the next version of a flow. The version number of the object being created will be the " +
                    "next available version integer. Flow versions are immutable after they are created.",
            response = VersionedFlowSnapshot.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 201, message = HttpStatusMessages.MESSAGE_201),
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409) })
    public Response importVersionedFlow(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
            final String bucketId,
            @PathParam("flowId")
            @ApiParam(value = "The flow identifier")
            final String flowId,
            @ApiParam("file") final VersionedFlowSnapshot versionedFlowSnapshot,
            @HeaderParam("Comments") final String comments) {

        final VersionedFlowSnapshot createdSnapshot = serviceFacade.importVersionedFlowSnapshot(versionedFlowSnapshot, bucketId, flowId, comments);
        publish(EventFactory.flowVersionCreated(createdSnapshot));
        String locationUri = createdSnapshot.getSnapshotMetadata().getLink().getUri().getPath();
        return generateCreatedResponse(URI.create(locationUri), createdSnapshot).build();
    }

    @GET
    @Path("{flowId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bucket flow versions",
            notes = "Gets summary information for all versions of a flow. Versions are ordered newest->oldest.",
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
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final SortedSet<VersionedFlowSnapshotMetadata> snapshots = serviceFacade.getFlowSnapshots(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(snapshots).build();
    }

    @GET
    @Path("{flowId}/versions/latest")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get latest bucket flow version content",
            notes = "Gets the latest version of a flow, including the metadata and content of the flow.",
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
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId) {

        final VersionedFlowSnapshot lastSnapshot = serviceFacade.getLatestFlowSnapshot(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(lastSnapshot).build();
    }

    @GET
    @Path("{flowId}/versions/latest/metadata")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get latest bucket flow version metadata",
            notes = "Gets the metadata for the latest version of a flow.",
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
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
            final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
            final String flowId) {

        final VersionedFlowSnapshotMetadata latest = serviceFacade.getLatestFlowSnapshotMetadata(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(latest).build();
    }

    @GET
    @Path("{flowId}/versions/{versionNumber: \\d+}/export")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Exports specified bucket flow version content",
            notes = "Exports the specified version of a flow, including the metadata and content of the flow.",
            response = VersionedFlowSnapshot.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")})
            }
    )
    @ApiResponses({
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409)})
    public Response exportVersionedFlow(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier") final String flowId,
            @PathParam("versionNumber")
            @ApiParam("The version number") final Integer versionNumber) {

        final ExportedVersionedFlowSnapshot exportedVersionedFlowSnapshot = serviceFacade.exportFlowSnapshot(bucketId, flowId, versionNumber);

        final VersionedFlowSnapshot versionedFlowSnapshot = exportedVersionedFlowSnapshot.getVersionedFlowSnapshot();

        final String contentDisposition = String.format(
                "attachment; filename=\"%s\"",
                exportedVersionedFlowSnapshot.getFilename());

        return generateOkResponse(versionedFlowSnapshot)
                .header(HttpHeaders.CONTENT_DISPOSITION, contentDisposition)
                .header("Filename", exportedVersionedFlowSnapshot.getFilename())
                .build();
    }

    @GET
    @Path("{flowId}/versions/{versionNumber: \\d+}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bucket flow version",
            notes = "Gets the given version of a flow, including the metadata and content for the version.",
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
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId,
            @PathParam("versionNumber")
            @ApiParam("The version number")
                final Integer versionNumber) {

        final VersionedFlowSnapshot snapshot = serviceFacade.getFlowSnapshot(bucketId, flowId, versionNumber);
        return Response.status(Response.Status.OK).entity(snapshot).build();
    }

    @GET
    @Path("{flowId}/diff/{versionA: \\d+}/{versionB: \\d+}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bucket flow diff",
            notes = "Computes the differences between two given versions of a flow.",
            response = VersionedFlowDifference.class,
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
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409)})
    public Response getFlowDiff(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @PathParam("flowId")
            @ApiParam("The flow identifier")
                final String flowId,
            @PathParam("versionA")
            @ApiParam("The first version number")
                final Integer versionNumberA,
            @PathParam("versionB")
            @ApiParam("The second version number")
                final Integer versionNumberB) {

        final VersionedFlowDifference result = serviceFacade.getFlowDiff(bucketId, flowId, versionNumberA, versionNumberB);
        return Response.status(Response.Status.OK).entity(result).build();
    }

    private static void verifyPathParamsMatchBody(String bucketIdParam, BucketItem bodyBucketItem) throws BadRequestException {
        if (StringUtils.isBlank(bucketIdParam)) {
            throw new BadRequestException("Bucket id path parameter cannot be blank");
        }

        if (bodyBucketItem == null) {
            throw new BadRequestException("Object in body cannot be null");
        }

        if (bodyBucketItem.getBucketIdentifier() != null && !bucketIdParam.equals(bodyBucketItem.getBucketIdentifier())) {
            throw new BadRequestException("Bucket id in path param must match bucket id in body");
        }
    }

    private static void verifyPathParamsMatchBody(String bucketIdParam, String flowIdParam, BucketItem bodyBucketItem) throws BadRequestException {
        verifyPathParamsMatchBody(bucketIdParam, bodyBucketItem);

        if (StringUtils.isBlank(flowIdParam)) {
            throw new BadRequestException("Flow id path parameter cannot be blank");
        }

        if (bodyBucketItem.getIdentifier() != null && !flowIdParam.equals(bodyBucketItem.getIdentifier())) {
            throw new BadRequestException("Item id in path param must match item id in body");
        }
    }

    private static void verifyPathParamsMatchBody(String bucketIdParam, String flowIdParam, VersionedFlowSnapshot flowSnapshot) throws BadRequestException {
        if (StringUtils.isBlank(bucketIdParam)) {
            throw new BadRequestException("Bucket id path parameter cannot be blank");
        }

        if (StringUtils.isBlank(flowIdParam)) {
            throw new BadRequestException("Flow id path parameter cannot be blank");
        }

        if (flowSnapshot == null) {
            throw new BadRequestException("VersionedFlowSnapshot cannot be null in body");
        }

        final VersionedFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        if (metadata != null && metadata.getBucketIdentifier() != null && !bucketIdParam.equals(metadata.getBucketIdentifier())) {
            throw new BadRequestException("Bucket id in path param must match bucket id in body");
        }
        if (metadata != null && metadata.getFlowIdentifier() != null && !flowIdParam.equals(metadata.getFlowIdentifier())) {
            throw new BadRequestException("Flow id in path param must match flow id in body");
        }
    }

    private static void setBucketItemMetadataIfMissing(
            @NotNull String bucketIdParam,
            @NotNull String bucketItemIdParam,
            @NotNull BucketItem bucketItem) {
        if (bucketItem.getBucketIdentifier() == null) {
            bucketItem.setBucketIdentifier(bucketIdParam);
        }

        if (bucketItem.getIdentifier() == null) {
            bucketItem.setIdentifier(bucketItemIdParam);
        }
    }

    private static void setSnaphotMetadataIfMissing(
            @NotNull String bucketIdParam,
            @NotNull String flowIdParam,
            @NotNull VersionedFlowSnapshot flowSnapshot) {

        VersionedFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        if (metadata == null) {
            metadata = new VersionedFlowSnapshotMetadata();
        }

        if (metadata.getBucketIdentifier() == null) {
            metadata.setBucketIdentifier(bucketIdParam);
        }

        if (metadata.getFlowIdentifier() == null) {
            metadata.setFlowIdentifier(flowIdParam);
        }

        flowSnapshot.setSnapshotMetadata(metadata);
    }
}
