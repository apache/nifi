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
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.SnippetAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;

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
import java.net.URI;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
     * Authorizes the specified snippet request with the specified request action. This method is used when creating a snippet. Because we do not know what
     * the snippet will be used for, we just ensure the user has permissions to each selected component. Some actions may require additional permissions
     * (including referenced services) but those will be enforced when the snippet is used.
     *
     * @param authorizer authorizer
     * @param lookup     lookup
     * @param action     action
     */
    private void authorizeSnippetRequest(final SnippetDTO snippetRequest, final Authorizer authorizer, final AuthorizableLookup lookup, final RequestAction action) {
        final Consumer<Authorizable> authorize = authorizable -> authorizable.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());

        // note - we are not authorizing templates or controller services as they are not considered when using this snippet
        snippetRequest.getProcessGroups().keySet().stream().map(id -> lookup.getProcessGroup(id)).forEach(processGroupAuthorizable -> {
            // we are not checking referenced services since we do not know how this snippet will be used. these checks should be performed
            // in a subsequent action with this snippet
            authorizeProcessGroup(processGroupAuthorizable, authorizer, lookup, action, false, false, false, false);
        });
        snippetRequest.getRemoteProcessGroups().keySet().stream().map(id -> lookup.getRemoteProcessGroup(id)).forEach(authorize);
        snippetRequest.getProcessors().keySet().stream().map(id -> lookup.getProcessor(id).getAuthorizable()).forEach(authorize);
        snippetRequest.getInputPorts().keySet().stream().map(id -> lookup.getInputPort(id)).forEach(authorize);
        snippetRequest.getOutputPorts().keySet().stream().map(id -> lookup.getOutputPort(id)).forEach(authorize);
        snippetRequest.getConnections().keySet().stream().map(id -> lookup.getConnection(id).getAuthorizable()).forEach(authorize);
        snippetRequest.getFunnels().keySet().stream().map(id -> lookup.getFunnel(id)).forEach(authorize);
        snippetRequest.getLabels().keySet().stream().map(id -> lookup.getLabel(id)).forEach(authorize);
    }

    /**
     * Creates a snippet based off the specified configuration.
     *
     * @param httpServletRequest request
     * @param requestSnippetEntity      A snippetEntity
     * @return A snippetEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Creates a snippet. The snippet will be automatically discarded if not used in a subsequent request after 1 minute.",
            response = SnippetEntity.class,
            authorizations = {
                    @Authorization(value = "Read or Write - /{component-type}/{uuid} - For every component (all Read or all Write) in the Snippet and their descendant components", type = "")
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
            final SnippetEntity requestSnippetEntity) {

        if (requestSnippetEntity == null || requestSnippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        if (requestSnippetEntity.getSnippet().getId() != null) {
            throw new IllegalArgumentException("Snippet ID cannot be specified.");
        }

        if (requestSnippetEntity.getSnippet().getParentGroupId() == null) {
            throw new IllegalArgumentException("The parent Process Group of the snippet must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestSnippetEntity);
        }

        return withWriteLock(
                serviceFacade,
                requestSnippetEntity,
                lookup -> {
                    final SnippetDTO snippetRequest = requestSnippetEntity.getSnippet();

                    // the snippet being created may be used later for batch component modifications,
                    // copy/paste, or template creation. during those subsequent actions, the snippet
                    // will again be authorized accordingly (read or write). at this point we do not
                    // know what the snippet will be used for so we need to attempt to authorize as
                    // read OR write

                    try {
                        authorizeSnippetRequest(snippetRequest, authorizer, lookup, RequestAction.READ);
                    } catch (final AccessDeniedException e) {
                        authorizeSnippetRequest(snippetRequest, authorizer, lookup, RequestAction.WRITE);
                    }
                },
                null,
                (snippetEntity) -> {
                    // set the processor id as appropriate
                    snippetEntity.getSnippet().setId(generateUuid());

                    // create the snippet
                    final SnippetEntity entity = serviceFacade.createSnippet(snippetEntity.getSnippet());
                    populateRemainingSnippetEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getSnippet().getUri()), entity).build();
                }
        );
    }

    /**
     * Move's the components in this Snippet into a new Process Group.
     *
     * @param httpServletRequest request
     * @param snippetId          The id of the snippet.
     * @param requestSnippetEntity      A snippetEntity
     * @return A snippetEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Move's the components in this Snippet into a new Process Group and discards the snippet",
            response = SnippetEntity.class,
            authorizations = {
                    @Authorization(value = "Write Process Group - /process-groups/{uuid}", type = ""),
                    @Authorization(value = "Write - /{component-type}/{uuid} - For each component in the Snippet and their descendant components", type = "")
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
            ) final SnippetEntity requestSnippetEntity) {

        if (requestSnippetEntity == null || requestSnippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        // ensure the ids are the same
        final SnippetDTO requestSnippetDTO = requestSnippetEntity.getSnippet();
        if (!snippetId.equals(requestSnippetDTO.getId())) {
            throw new IllegalArgumentException(String.format("The snippet id (%s) in the request body does not equal the "
                    + "snippet id of the requested resource (%s).", requestSnippetDTO.getId(), snippetId));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestSnippetEntity);
        }

        // get the revision from this snippet
        final Set<Revision> requestRevisions = serviceFacade.getRevisionsFromSnippet(snippetId);
        return withWriteLock(
                serviceFacade,
                requestSnippetEntity,
                requestRevisions,
                lookup -> {
                    // ensure write access to the target process group
                    if (requestSnippetDTO.getParentGroupId() != null) {
                        lookup.getProcessGroup(requestSnippetDTO.getParentGroupId()).getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                    }

                    // ensure write permission to every component in the snippet excluding referenced services
                    final SnippetAuthorizable snippet = lookup.getSnippet(snippetId);
                    authorizeSnippet(snippet, authorizer, lookup, RequestAction.WRITE, false, false);
                },
                () -> serviceFacade.verifyUpdateSnippet(requestSnippetDTO, requestRevisions.stream().map(rev -> rev.getComponentId()).collect(Collectors.toSet())),
                (revisions, snippetEntity) -> {
                    // update the snippet
                    final SnippetEntity entity = serviceFacade.updateSnippet(revisions, snippetEntity.getSnippet());
                    populateRemainingSnippetEntityContent(entity);
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified snippet.
     *
     * @param httpServletRequest request
     * @param snippetId          The id of the snippet to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes the components in a snippet and discards the snippet",
            response = SnippetEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /{component-type}/{uuid} - For each component in the Snippet and their descendant components", type = ""),
                    @Authorization(value = "Write - Parent Process Group - /process-groups/{uuid}", type = ""),
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

        final ComponentEntity requestEntity = new ComponentEntity();
        requestEntity.setId(snippetId);

        // get the revision from this snippet
        final Set<Revision> requestRevisions = serviceFacade.getRevisionsFromSnippet(snippetId);
        return withWriteLock(
                serviceFacade,
                requestEntity,
                requestRevisions,
                lookup -> {
                    // ensure write permission to every component in the snippet excluding referenced services
                    final SnippetAuthorizable snippet = lookup.getSnippet(snippetId);
                    authorizeSnippet(snippet, authorizer, lookup, RequestAction.WRITE, true, false);

                    // ensure write permission to the parent process group
                    snippet.getParentProcessGroup().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteSnippet(snippetId, requestRevisions.stream().map(rev -> rev.getComponentId()).collect(Collectors.toSet())),
                (revisions, entity) -> {
                    // delete the specified snippet
                    final SnippetEntity snippetEntity = serviceFacade.deleteSnippet(revisions, entity.getId());
                    return generateOkResponse(snippetEntity).build();
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
