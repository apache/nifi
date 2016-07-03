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

import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.entity.SnippetEntity;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;

/**
 * RESTful endpoint for querying dataflow snippets.
 */
@Path("/snippets")
@Api(
    value = "/snippets",
    description = "Endpoint for accessing dataflow snippets."
)
public class SnippetResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populate the uri's for the specified snippet.
     *
     * @param entity processors
     * @return dtos
     */
    private SnippetEntity populateRemainingSnippetEntityContent(SnippetEntity entity) {
        if (entity.getSnippet() != null) {
            populateRemainingSnippetContent(entity.getSnippet());
        }
        return entity;
    }

    /**
     * Populates the uri for the specified snippet.
     */
    private SnippetDTO populateRemainingSnippetContent(SnippetDTO snippet) {
        String snippetGroupId = snippet.getParentGroupId();

        // populate the snippet href
        snippet.setUri(generateResourceUri("process-groups", snippetGroupId, "snippets", snippet.getId()));

        return snippet;
    }

    // --------
    // snippets
    // --------

    /**
     * Creates a snippet based off the specified configuration.
     *
     * @param httpServletRequest request
     * @param snippetEntity A snippetEntity
     * @return A snippetEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a snippet",
        response = SnippetEntity.class,
        authorizations = {
            @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
            @Authorization(value = "Administrator", type = "ROLE_ADMIN")
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
    public Response createSnippet(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The snippet configuration details.",
            required = true
        )
        final SnippetEntity snippetEntity) {

        if (snippetEntity == null || snippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        if (snippetEntity.getSnippet().getId() != null) {
            throw new IllegalArgumentException("Snippet ID cannot be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, snippetEntity);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final SnippetDTO snippet = snippetEntity.getSnippet();
                authorizeSnippet(snippet, authorizer, lookup, RequestAction.READ);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        snippetEntity.getSnippet().setId(generateUuid());

        // create the snippet
        final SnippetEntity entity = serviceFacade.createSnippet(snippetEntity.getSnippet());
        populateRemainingSnippetEntityContent(entity);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(entity.getSnippet().getUri()), entity)).build();
    }

    /**
     * Updates the specified snippet. The contents of the snippet (component
     * ids) cannot be updated once the snippet is created.
     *
     * @param httpServletRequest request
     * @param snippetId The id of the snippet.
     * @param snippetEntity A snippetEntity
     * @return A snippetEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Updates a snippet",
        response = SnippetEntity.class,
        authorizations = {
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response updateSnippet(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The snippet id.",
            required = true
        )
        @PathParam("id") String snippetId,
        @ApiParam(
            value = "The snippet configuration details.",
            required = true
        ) final SnippetEntity snippetEntity) {

        if (snippetEntity == null || snippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        // ensure the ids are the same
        final SnippetDTO requestSnippetDTO = snippetEntity.getSnippet();
        if (!snippetId.equals(requestSnippetDTO.getId())) {
            throw new IllegalArgumentException(String.format("The snippet id (%s) in the request body does not equal the "
                + "snippet id of the requested resource (%s).", requestSnippetDTO.getId(), snippetId));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, snippetEntity);
        }

        // get the revision from this snippet
        final Set<Revision> revisions = serviceFacade.getRevisionsFromSnippet(snippetId);
        return withWriteLock(
            serviceFacade,
            revisions,
            lookup -> {
                // ensure write access to the target process group
                if (requestSnippetDTO.getParentGroupId() != null) {
                    lookup.getProcessGroup(requestSnippetDTO.getParentGroupId()).authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                }

                // ensure read permission to every component in the snippet
                final Snippet snippet = lookup.getSnippet(snippetId);
                authorizeSnippet(snippet, authorizer, lookup, RequestAction.WRITE);
            },
            () -> serviceFacade.verifyUpdateSnippet(requestSnippetDTO, revisions.stream().map(rev -> rev.getComponentId()).collect(Collectors.toSet())),
            () -> {
                // update the snippet
                final SnippetEntity entity = serviceFacade.updateSnippet(revisions, snippetEntity.getSnippet());
                populateRemainingSnippetEntityContent(entity);
                return clusterContext(generateOkResponse(entity)).build();
            }
        );
    }

    /**
     * Removes the specified snippet.
     *
     * @param httpServletRequest request
     * @param snippetId The id of the snippet to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Deletes the components in a snippet and drops the snippet",
        response = SnippetEntity.class,
        authorizations = {
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response deleteSnippet(
        @Context final HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The snippet id.",
            required = true
        )
        @PathParam("id") final String snippetId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        // get the revision from this snippet
        final Set<Revision> revisions = serviceFacade.getRevisionsFromSnippet(snippetId);
        return withWriteLock(
            serviceFacade,
            revisions,
            lookup -> {
                // ensure read permission to every component in the snippet
                final Snippet snippet = lookup.getSnippet(snippetId);
                authorizeSnippet(snippet, authorizer, lookup, RequestAction.WRITE);
            },
            () -> serviceFacade.verifyDeleteSnippet(snippetId, revisions.stream().map(rev -> rev.getComponentId()).collect(Collectors.toSet())),
            () -> {
                // delete the specified snippet
                final SnippetEntity snippetEntity = serviceFacade.deleteSnippet(revisions, snippetId);
                return clusterContext(generateOkResponse(snippetEntity)).build();
            }
        );
    }

    /* setters */
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
