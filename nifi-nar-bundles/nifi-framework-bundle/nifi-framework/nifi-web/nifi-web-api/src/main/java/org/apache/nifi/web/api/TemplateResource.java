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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.persistence.TemplateSerializer;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.TemplateEntity;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.charset.Charset;
import java.util.Set;

/**
 * RESTful endpoint for managing a Template.
 */
@Path("/templates")
@Api(
        value = "/templates",
        description = "Endpoint for managing a Template."
)
public class TemplateResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populate the uri's for the specified templates.
     *
     * @param templateEntities templates
     * @return templates
     */
    public Set<TemplateEntity> populateRemainingTemplateEntitiesContent(Set<TemplateEntity> templateEntities) {
        for (TemplateEntity templateEntity : templateEntities) {
            if (templateEntity.getTemplate() != null) {
                populateRemainingTemplateContent(templateEntity.getTemplate());
            }
        }
        return templateEntities;
    }

    /**
     * Populates the uri for the specified template.
     */
    public TemplateDTO populateRemainingTemplateContent(TemplateDTO template) {
        // populate the template uri
        template.setUri(generateResourceUri("templates", template.getId()));
        return template;
    }

    /**
     * Retrieves the specified template.
     *
     * @param id The id of the template to retrieve
     * @return A templateEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_XML)
    @Path("{id}/download")
    @ApiOperation(
            value = "Exports a template",
            response = String.class,
            authorizations = {
                    @Authorization(value = "Read - /templates/{uuid}")
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
    public Response exportTemplate(
            @ApiParam(
                    value = "The template id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable template = lookup.getTemplate(id);
            template.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the template
        final TemplateDTO template = serviceFacade.exportTemplate(id);

        // prune the template id
        template.setId(null);

        // determine the name of the attachement - possible issues with spaces in file names
        String attachmentName = template.getName();
        if (StringUtils.isBlank(attachmentName)) {
            attachmentName = "template";
        } else {
            attachmentName = attachmentName.replaceAll("\\s", "_");
        }

        final Charset utf8 = StandardCharsets.UTF_8;
        try {
            attachmentName = URLEncoder.encode(attachmentName, utf8.name());
        } catch (UnsupportedEncodingException e) {
            //
        }

        // generate the response
        /*
         * Here instead of relying on default JAXB marshalling we are simply
         * serializing template to String (formatted, indented etc) and sending
         * it as part of the response.
         */
        String serializedTemplate = new String(TemplateSerializer.serialize(template), utf8);
        String filename = attachmentName + ".xml";
        return generateOkResponse(serializedTemplate).encoding(utf8.name()).header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename* = " + utf8.name() + "''" + filename).build();
    }

    /**
     * Removes the specified template.
     *
     * @param httpServletRequest request
     * @param id                 The id of the template to remove.
     * @return A templateEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes a template",
            response = TemplateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /templates/{uuid}"),
                    @Authorization(value = "Write - Parent Process Group - /process-groups/{uuid}")
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
    public Response removeTemplate(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam(
                    value = "The template id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final TemplateEntity requestTemplateEntity = new TemplateEntity();
        requestTemplateEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestTemplateEntity,
                lookup -> {
                    final Authorizable template = lookup.getTemplate(id);

                    // ensure write permission to the template
                    template.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    template.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (templateEntity) -> {
                    // delete the specified template
                    serviceFacade.deleteTemplate(templateEntity.getId());

                    // build the response entity
                    final TemplateEntity entity = new TemplateEntity();

                    return generateOkResponse(entity).build();
                }
        );
    }

    // setters

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
