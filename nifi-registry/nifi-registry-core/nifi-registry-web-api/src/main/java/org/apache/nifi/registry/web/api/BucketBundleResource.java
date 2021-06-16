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
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleTypeValues;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Component
@Path("/buckets/{bucketId}/bundles")
@Api(
        value = "bucket bundles",
        description = "Create extension bundles scoped to an existing bucket in the registry. ",
        authorizations = { @Authorization("Authorization") }
)
public class BucketBundleResource extends ApplicationResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketBundleResource.class);

    @Autowired
    public BucketBundleResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @POST
    @Path("{bundleType}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Create extension bundle version",
            notes =  "Creates a version of an extension bundle by uploading a binary artifact. " +
                    "If an extension bundle already exists in the given bucket with the same group id and artifact id " +
                    "as that of the bundle being uploaded, then it will be added as a new version to the existing bundle. " +
                    "If an extension bundle does not already exist in the given bucket with the same group id and artifact id, " +
                    "then a new extension bundle will be created and this version will be added to the new bundle. " +
                    "Client's may optionally supply a SHA-256 in hex format through the multi-part form field 'sha256'. " +
                    "If supplied, then this value will be compared against the SHA-256 computed by the server, and the bundle " +
                    "will be rejected if the values do not match. If not supplied, the bundle will be accepted, but will be marked " +
                    "to indicate that the client did not supply a SHA-256 during creation. " + NON_GUARANTEED_ENDPOINT,
            response = BundleVersion.class,
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
    public Response createExtensionBundleVersion(
            @PathParam("bucketId")
            @ApiParam(value = "The bucket identifier", required = true)
                final String bucketId,
            @PathParam("bundleType")
            @ApiParam(value = "The type of the bundle", required = true, allowableValues = BundleTypeValues.ALL_VALUES)
                final BundleType bundleType,
            @FormDataParam("file")
                final InputStream fileInputStream,
            @FormDataParam("file")
                final FormDataContentDisposition fileMetaData,
            @FormDataParam("sha256")
                final String clientSha256) throws IOException {

        LOGGER.debug("Creating extension bundle version for bundle type {}", new Object[]{bundleType});

        final BundleVersion createdBundleVersion = serviceFacade.createBundleVersion(
                bucketId, bundleType, fileInputStream, clientSha256);

        publish(EventFactory.extensionBundleCreated(createdBundleVersion.getBundle()));
        publish(EventFactory.extensionBundleVersionCreated(createdBundleVersion));

        return Response.status(Response.Status.OK).entity(createdBundleVersion).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get extension bundles by bucket",
            notes = NON_GUARANTEED_ENDPOINT,
            response = Bundle.class,
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
    public Response getExtensionBundles(
            @PathParam("bucketId")
            @ApiParam(value = "The bucket identifier", required = true)
                final String bucketId) {

        final List<Bundle> bundles = serviceFacade.getBundlesByBucket(bucketId);
        return Response.status(Response.Status.OK).entity(bundles).build();
    }

}
