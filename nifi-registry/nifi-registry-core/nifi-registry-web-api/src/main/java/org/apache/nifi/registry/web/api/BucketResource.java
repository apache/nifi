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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.revision.web.ClientIdParameter;
import org.apache.nifi.registry.revision.web.LongParameter;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
import java.util.Set;

@Component
@Path("/buckets")
@Api(
        value = "buckets",
        description = "Create named buckets in the registry to store NiFi objects such flows and extensions. " +
                "Search for and retrieve existing buckets.",
        authorizations = { @Authorization("Authorization") }
)
public class BucketResource extends ApplicationResource {

    @Autowired
    public BucketResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Create bucket",
            response = Bucket.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403) })
    public Response createBucket(
            @ApiParam(value = "The bucket to create", required = true)
            final Bucket bucket) {

        final Bucket createdBucket = serviceFacade.createBucket(bucket);
        publish(EventFactory.bucketCreated(createdBucket));
        return Response.status(Response.Status.OK).entity(createdBucket).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get all buckets",
            notes = "The returned list will include only buckets for which the user is authorized." +
                    "If the user is not authorized for any buckets, this returns an empty list.",
            response = Bucket.class,
            responseContainer = "List"
    )
    @ApiResponses({ @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401) })
    public Response getBuckets() {
        // ServiceFacade will determine which buckets the user is authorized for
        // Note: We don't explicitly check for access to (READ, /buckets) because
        // a user might have access to individual buckets without top-level access.
        // For example, a user that has (READ, /buckets/bucket-id-1) but not access
        // to /buckets should not get a 403 error returned from this endpoint.
        // This has the side effect that a user with no access to any buckets
        // gets an empty array returned from this endpoint instead of 403 as one
        // might expect.
        final List<Bucket> buckets = serviceFacade.getBuckets();
        return Response.status(Response.Status.OK).entity(buckets).build();
    }

    @GET
    @Path("{bucketId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bucket",
            notes = "Gets the bucket with the given id.",
            response = Bucket.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404) })
    public Response getBucket(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
            final String bucketId) {

        final Bucket bucket = serviceFacade.getBucket(bucketId);
        return Response.status(Response.Status.OK).entity(bucket).build();
    }

    @PUT
    @Path("{bucketId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update bucket",
            notes = "Updates the bucket with the given id.",
            response = Bucket.class,
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
    public Response updateBucket(
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId,
            @ApiParam(value = "The updated bucket", required = true)
                final Bucket bucket) {

        if (StringUtils.isBlank(bucketId)) {
            throw new BadRequestException("Bucket id cannot be blank");
        }

        if (bucket == null) {
            throw new BadRequestException("Bucket cannot be null");
        }

        if (bucket.getIdentifier() != null && !bucketId.equals(bucket.getIdentifier())) {
            throw new BadRequestException("Bucket id in path param must match bucket id in body");
        } else {
            bucket.setIdentifier(bucketId);
        }

        final Bucket updatedBucket = serviceFacade.updateBucket(bucket);
        publish(EventFactory.bucketUpdated(updatedBucket));
        return Response.status(Response.Status.OK).entity(updatedBucket).build();
    }

    @DELETE
    @Path("{bucketId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Delete bucket",
            notes = "Deletes the bucket with the given id, along with all objects stored in the bucket",
            response = Bucket.class,
            extensions = {
                    @Extension(name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "delete"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}") })
            }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 403, message = HttpStatusMessages.MESSAGE_403),
            @ApiResponse(code = 404, message = HttpStatusMessages.MESSAGE_404) })
    public Response deleteBucket(
            @ApiParam(value = "The version is used to verify the client is working with the latest version of the entity.", required = true)
            @QueryParam(VERSION)
                final LongParameter version,
            @ApiParam(value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY)
                final ClientIdParameter clientId,
            @PathParam("bucketId")
            @ApiParam("The bucket identifier")
                final String bucketId) {

        if (StringUtils.isBlank(bucketId)) {
            throw new BadRequestException("Bucket id cannot be blank");
        }

        final RevisionInfo revisionInfo = getRevisionInfo(version, clientId);
        final Bucket deletedBucket = serviceFacade.deleteBucket(bucketId, revisionInfo);
        publish(EventFactory.bucketDeleted(deletedBucket));

        return Response.status(Response.Status.OK).entity(deletedBucket).build();
    }

    @GET
    @Path("fields")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bucket fields",
            notes = "Retrieves bucket field names for searching or sorting on buckets.",
            response = Fields.class
    )
    public Response getAvailableBucketFields() {
        final Set<String> bucketFields = serviceFacade.getBucketFields();
        final Fields fields = new Fields(bucketFields);
        return Response.status(Response.Status.OK).entity(fields).build();
    }

}
