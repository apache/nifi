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
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.apache.nifi.registry.web.service.StreamingContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.util.List;
import java.util.SortedSet;

@Component
@Path("/bundles")
@Api(
        value = "bundles",
        description = "Gets metadata about extension bundles and their versions. ",
        authorizations = { @Authorization("Authorization") }
)
public class BundleResource extends ApplicationResource {

    public static final String CONTENT_DISPOSITION_HEADER = "content-disposition";

    @Autowired
    public BundleResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    // ---------- Extension Bundles ----------

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get all bundles",
            notes = "Gets the metadata for all bundles across all authorized buckets with optional filters applied. " +
                    "The returned results will include only items from buckets for which the user is authorized. " +
                    "If the user is not authorized to any buckets, an empty list will be returned. " + NON_GUARANTEED_ENDPOINT,
            response = Bundle.class,
            responseContainer = "List"
    )
    @ApiResponses({ @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401) })
    public Response getBundles(
            @QueryParam("bucketName")
            @ApiParam("Optional bucket name to filter results. The value may be an exact match, or a wildcard, " +
                    "such as 'My Bucket%' to select all bundles where the bucket name starts with 'My Bucket'.")
                final String bucketName,
            @QueryParam("groupId")
            @ApiParam("Optional groupId to filter results. The value may be an exact match, or a wildcard, " +
                    "such as 'com.%' to select all bundles where the groupId starts with 'com.'.")
                final String groupId,
            @QueryParam("artifactId")
            @ApiParam("Optional artifactId to filter results. The value may be an exact match, or a wildcard, " +
                    "such as 'nifi-%' to select all bundles where the artifactId starts with 'nifi-'.")
                final String artifactId) {

        final BundleFilterParams filterParams = BundleFilterParams.of(bucketName, groupId, artifactId);

        // Service facade will return only bundles from authorized buckets
        final List<Bundle> bundles = serviceFacade.getBundles(filterParams);
        return Response.status(Response.Status.OK).entity(bundles).build();
    }

    @GET
    @Path("{bundleId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bundle",
            notes = "Gets the metadata about an extension bundle. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalGetExtensionBundle",
            response = Bundle.class,
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
    public Response getBundle(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId) {

        final Bundle bundle = serviceFacade.getBundle(bundleId);
        return Response.status(Response.Status.OK).entity(bundle).build();
    }

    @DELETE
    @Path("{bundleId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Delete bundle",
            notes = "Deletes the given extension bundle and all of it's versions. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalDeleteExtensionBundle",
            response = Bundle.class,
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
    public Response deleteBundle(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId) {

        final Bundle deletedBundle = serviceFacade.deleteBundle(bundleId);
        publish(EventFactory.extensionBundleDeleted(deletedBundle));
        return Response.status(Response.Status.OK).entity(deletedBundle).build();
    }

    // ---------- Extension Bundle Versions ----------

    @GET
    @Path("versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get all bundle versions",
            notes = "Gets the metadata about extension bundle versions across all authorized buckets with optional filters applied. " +
                    "If the user is not authorized to any buckets, an empty list will be returned. " + NON_GUARANTEED_ENDPOINT,
            response = BundleVersionMetadata.class,
            responseContainer = "List"
    )
    @ApiResponses({ @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401) })
    public Response getBundleVersions(
            @QueryParam("groupId")
            @ApiParam("Optional groupId to filter results. The value may be an exact match, or a wildcard, " +
                    "such as 'com.%' to select all bundle versions where the groupId starts with 'com.'.")
                final String groupId,
            @QueryParam("artifactId")
            @ApiParam("Optional artifactId to filter results. The value may be an exact match, or a wildcard, " +
                    "such as 'nifi-%' to select all bundle versions where the artifactId starts with 'nifi-'.")
                final String artifactId,
            @QueryParam("version")
            @ApiParam("Optional version to filter results. The value maye be an exact match, or a wildcard, " +
                    "such as '1.0.%' to select all bundle versions where the version starts with '1.0.'.")
                final String version
            ) {

        final BundleVersionFilterParams filterParams = BundleVersionFilterParams.of(groupId, artifactId, version);
        final SortedSet<BundleVersionMetadata> bundleVersions = serviceFacade.getBundleVersions(filterParams);
        return Response.status(Response.Status.OK).entity(bundleVersions).build();
    }

    @GET
    @Path("{bundleId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bundle versions",
            notes = "Gets the metadata for the versions of the given extension bundle. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalGetBundleVersions",
            response = BundleVersionMetadata.class,
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
    public Response getBundleVersions(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId) {

        final SortedSet<BundleVersionMetadata> bundleVersions = serviceFacade.getBundleVersions(bundleId);
        return Response.status(Response.Status.OK).entity(bundleVersions).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bundle version",
            notes = "Gets the descriptor for the given version of the given extension bundle. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalGetBundleVersion",
            response = BundleVersion.class,
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
    public Response getBundleVersion(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId,
            @PathParam("version")
            @ApiParam("The version of the bundle")
                final String version) {

        final BundleVersion bundleVersion = serviceFacade.getBundleVersion(bundleId, version);
        return Response.ok(bundleVersion).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/content")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @ApiOperation(
            value = "Get bundle version content",
            notes = "Gets the binary content for the given version of the given extension bundle. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalGetBundleVersionContent",
            response = byte[].class,
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
    public Response getBundleVersionContent(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId,
            @PathParam("version")
            @ApiParam("The version of the bundle")
                final String version) {

        final StreamingContent streamingContent = serviceFacade.getBundleVersionContent(bundleId, version);

        final String filename = streamingContent.getFilename();
        final StreamingOutput output = streamingContent.getOutput();

        return Response.ok(output)
                .header(CONTENT_DISPOSITION_HEADER,"attachment; filename = " + filename)
                .build();
    }

    @DELETE
    @Path("{bundleId}/versions/{version}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Delete bundle version",
            notes = "Deletes the given extension bundle version and it's associated binary content. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalDeleteBundleVersion",
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
    public Response deleteBundleVersion(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId,
            @PathParam("version")
            @ApiParam("The version of the bundle")
                final String version) {

        final BundleVersion deletedBundleVersion = serviceFacade.deleteBundleVersion(bundleId, version);
        publish(EventFactory.extensionBundleVersionDeleted(deletedBundleVersion));
        return Response.status(Response.Status.OK).entity(deletedBundleVersion).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bundle version extensions",
            notes = "Gets the metadata about the extensions in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalGetBundleVersionExtensions",
            response = ExtensionMetadata.class,
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
    public Response getBundleVersionExtensions(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId,
            @PathParam("version")
            @ApiParam("The version of the bundle")
                final String version) {

        final SortedSet<ExtensionMetadata> extensions = serviceFacade.getExtensionMetadata(bundleId, version);
        return Response.ok(extensions).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions/{name}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get bundle version extension",
            notes = "Gets the metadata about the extension with the given name in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            nickname = "globalGetBundleVersionExtension",
            response = org.apache.nifi.registry.extension.component.manifest.Extension.class,
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
    public Response getBundleVersionExtension(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId,
            @PathParam("version")
            @ApiParam("The version of the bundle")
                final String version,
            @PathParam("name")
            @ApiParam("The fully qualified name of the extension")
                final String name
            ) {

        final org.apache.nifi.registry.extension.component.manifest.Extension extension =
                serviceFacade.getExtension(bundleId, version, name);
        return Response.ok(extension).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions/{name}/docs")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_HTML)
    @ApiOperation(
            value = "Get bundle version extension docs",
            notes = "Gets the documentation for the given extension in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            response = String.class,
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
    public Response getBundleVersionExtensionDocs(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId,
            @PathParam("version")
            @ApiParam("The version of the bundle")
                final String version,
            @PathParam("name")
            @ApiParam("The fully qualified name of the extension")
                final String name
    ) {
        final StreamingOutput streamingOutput = serviceFacade.getExtensionDocs(bundleId, version, name);
        return Response.ok(streamingOutput).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions/{name}/docs/additional-details")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_HTML)
    @ApiOperation(
            value = "Get bundle version extension docs details",
            notes = "Gets the additional details documentation for the given extension in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            response = String.class,
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
    public Response getBundleVersionExtensionAdditionalDetailsDocs(
            @PathParam("bundleId")
            @ApiParam("The extension bundle identifier")
                final String bundleId,
            @PathParam("version")
            @ApiParam("The version of the bundle")
                final String version,
            @PathParam("name")
            @ApiParam("The fully qualified name of the extension")
                final String name
    ) {
        final StreamingOutput streamingOutput = serviceFacade.getAdditionalDetailsDocs(bundleId, version, name);
        return Response.ok(streamingOutput).build();
    }

}
