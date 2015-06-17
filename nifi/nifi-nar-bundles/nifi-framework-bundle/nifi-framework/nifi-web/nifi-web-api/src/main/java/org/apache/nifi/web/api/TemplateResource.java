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

import com.sun.jersey.multipart.FormDataParam;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import static org.apache.nifi.web.api.ApplicationResource.CLIENT_ID;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.apache.nifi.web.api.entity.TemplatesEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Template.
 */
@Api(hidden = true)
public class TemplateResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(TemplateResource.class);

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    /**
     * Populates the uri for the specified templates.
     *
     * @param templates templates
     * @return templates
     */
    public Set<TemplateDTO> populateRemainingTemplatesContent(Set<TemplateDTO> templates) {
        for (TemplateDTO template : templates) {
            populateRemainingTemplateContent(template);
        }
        return templates;
    }

    /**
     * Populates the uri for the specified template.
     */
    private TemplateDTO populateRemainingTemplateContent(TemplateDTO template) {
        // populate the template uri
        template.setUri(generateResourceUri("controller", "templates", template.getId()));
        return template;
    }

    /**
     * Retrieves all the of templates in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A templatesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets all templates",
            response = TemplatesEntity.class,
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
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getTemplates(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the templates
        final Set<TemplateDTO> templates = populateRemainingTemplatesContent(serviceFacade.getTemplates());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final TemplatesEntity entity = new TemplatesEntity();
        entity.setRevision(revision);
        entity.setTemplates(templates);
        entity.setGenerated(new Date());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new template based off of the specified template.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param name The name of the template.
     * @param description The description of the template.
     * @param snippetId The id of the snippet this template is based on.
     * @return A templateEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a template",
            response = TemplateEntity.class,
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
    public Response createTemplate(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The template name.",
                    required = true
            )
            @FormParam("name") String name,
            @ApiParam(
                    value = "The template description.",
                    required = false
            )
            @FormParam("description") String description,
            @ApiParam(
                    value = "The id of the snippet whose contents will comprise the template.",
                    required = true
            )
            @FormParam("snippetId") String snippetId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the template and generate the json
        final TemplateDTO template = serviceFacade.createTemplate(name, description, snippetId);
        populateRemainingTemplateContent(template);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // build the response entity
        final TemplateEntity entity = new TemplateEntity();
        entity.setRevision(revision);
        entity.setTemplate(template);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(template.getUri()), entity)).build();
    }

    /**
     * Imports the specified template.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param in The template stream
     * @return A templateEntity or an errorResponse XML snippet.
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_XML)
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response importTemplate(
            @Context HttpServletRequest httpServletRequest,
            @FormDataParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormDataParam("template") InputStream in) {

        // unmarshal the template
        final TemplateDTO template;
        try {
            JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            JAXBElement<TemplateDTO> templateElement = unmarshaller.unmarshal(new StreamSource(in), TemplateDTO.class);
            template = templateElement.getValue();
        } catch (JAXBException jaxbe) {
            logger.warn("An error occurred while parsing a template.", jaxbe);
            String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"The specified template is not in a valid format.\"/>", Response.Status.BAD_REQUEST.getStatusCode());
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        } catch (IllegalArgumentException iae) {
            logger.warn("Unable to import template.", iae);
            String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"%s\"/>", Response.Status.BAD_REQUEST.getStatusCode(), iae.getMessage());
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        } catch (Exception e) {
            logger.warn("An error occurred while importing a template.", e);
            String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"Unable to import the specified template: %s\"/>",
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // build the response entity
        TemplateEntity entity = new TemplateEntity();
        entity.setRevision(revision);
        entity.setTemplate(template);

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(entity), getHeaders(headersToOverride)).getResponse();
        }

        // otherwise import the template locally
        return importTemplate(httpServletRequest, entity);
    }

    /**
     * Imports the specified template.
     *
     * @param httpServletRequest request
     * @param templateEntity A templateEntity.
     * @return A templateEntity.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces(MediaType.APPLICATION_XML)
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response importTemplate(
            @Context HttpServletRequest httpServletRequest,
            TemplateEntity templateEntity) {

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        try {
            // verify the template was specified
            if (templateEntity == null || templateEntity.getTemplate() == null) {
                throw new IllegalArgumentException("Template details must be specified.");
            }

            // import the template
            final TemplateDTO template = serviceFacade.importTemplate(templateEntity.getTemplate());
            populateRemainingTemplateContent(template);

            // create the revision
            final RevisionDTO revision = new RevisionDTO();
            if (templateEntity.getRevision() == null) {
                revision.setClientId(new ClientIdParameter().getClientId());
            } else {
                revision.setClientId(templateEntity.getRevision().getClientId());
            }

            // build the response entity
            TemplateEntity entity = new TemplateEntity();
            entity.setRevision(revision);
            entity.setTemplate(template);

            // build the response
            return clusterContext(generateCreatedResponse(URI.create(template.getUri()), entity)).build();
        } catch (IllegalArgumentException | IllegalStateException e) {
            logger.info("Unable to import template: " + e);
            String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"%s\"/>", Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        } catch (Exception e) {
            logger.warn("An error occurred while importing a template.", e);
            String responseXml
                    = String.format("<errorResponse status=\"%s\" statusText=\"Unable to import the specified template: %s\"/>", Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        }
    }

    /**
     * Retrieves the specified template.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the template to retrieve
     * @return A templateEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_XML)
    @Path("{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Exports a template",
            response = TemplateDTO.class,
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
    public Response exportTemplate(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The template id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

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

        // generate the response
        return generateOkResponse(template).header("Content-Disposition", String.format("attachment; filename=\"%s.xml\"", attachmentName)).build();
    }

    /**
     * Removes the specified template.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the template to remove.
     * @return A templateEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a template",
            response = TemplateEntity.class,
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
    public Response removeTemplate(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The template id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // delete the specified template
        serviceFacade.deleteTemplate(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // build the response entity
        final TemplateEntity entity = new TemplateEntity();
        entity.setRevision(revision);

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
