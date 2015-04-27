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
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LabelsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DoubleParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Label.
 */
public class LabelResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(LabelResource.class);

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
    private String groupId;

    /**
     * Populates the uri for the specified labels.
     *
     * @param labels
     * @return
     */
    public Set<LabelDTO> populateRemainingLabelsContent(Set<LabelDTO> labels) {
        for (LabelDTO label : labels) {
            populateRemainingLabelContent(label);
        }
        return labels;
    }

    /**
     * Populates the uri for the specified label.
     */
    private LabelDTO populateRemainingLabelContent(LabelDTO label) {
        // populate the label href
        label.setUri(generateResourceUri("controller", "process-groups", groupId, "labels", label.getId()));
        return label;
    }

    /**
     * Retrieves all the of labels in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A labelsEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(LabelsEntity.class)
    public Response getLabels(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the labels
        final Set<LabelDTO> labels = populateRemainingLabelsContent(serviceFacade.getLabels(groupId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final LabelsEntity entity = new LabelsEntity();
        entity.setRevision(revision);
        entity.setLabels(labels);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new label.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @param width The width of the label.
     * @param height The height of the label.
     * @param label The label's value.
     * @return A labelEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(LabelEntity.class)
    public Response createLabel(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y,
            @FormParam("width") DoubleParameter width, @FormParam("height") DoubleParameter height,
            @FormParam("label") String label) {

        // ensure the position has been specified
        if (x == null || y == null) {
            throw new IllegalArgumentException("The position (x, y) must be specified");
        }

        // ensure the size has been specified
        if (width == null || height == null) {
            throw new IllegalArgumentException("The size (width, height) must be specified.");
        }

        // create the label DTO
        final LabelDTO labelDTO = new LabelDTO();
        labelDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));
        labelDTO.setWidth(width.getDouble());
        labelDTO.setHeight(height.getDouble());
        labelDTO.setLabel(label);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the label entity
        final LabelEntity labelEntity = new LabelEntity();
        labelEntity.setRevision(revision);
        labelEntity.setLabel(labelDTO);

        // create the label
        return createLabel(httpServletRequest, labelEntity);
    }

    /**
     * Creates a new Label.
     *
     * @param httpServletRequest
     * @param labelEntity A labelEntity.
     * @return A labelEntity.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(LabelEntity.class)
    public Response createLabel(
            @Context HttpServletRequest httpServletRequest,
            LabelEntity labelEntity) {

        if (labelEntity == null || labelEntity.getLabel() == null) {
            throw new IllegalArgumentException("Label details must be specified.");
        }

        if (labelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (labelEntity.getLabel().getId() != null) {
            throw new IllegalArgumentException("Label ID cannot be specified.");
        }

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager()) {

            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            labelEntity.getLabel().setId(id);

            // convert POST request to PUT request to force entity ID to be the same across nodes
            URI putUri = null;
            try {
                putUri = new URI(getAbsolutePath().toString() + "/" + id);
            } catch (final URISyntaxException e) {
                throw new WebApplicationException(e);
            }

            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate put request
            return (Response) clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(labelEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the label and generate the json
        final RevisionDTO revision = labelEntity.getRevision();
        final ConfigurationSnapshot<LabelDTO> controllerResponse = serviceFacade.createLabel(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, labelEntity.getLabel());
        final LabelDTO label = controllerResponse.getConfiguration();
        populateRemainingLabelContent(label);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final LabelEntity entity = new LabelEntity();
        entity.setRevision(updatedRevision);
        entity.setLabel(label);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(label.getUri()), entity)).build();
    }

    /**
     * Retrieves the specified label.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the label to retrieve
     * @return A labelEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(LabelEntity.class)
    public Response getLabel(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId, @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the label
        final LabelDTO label = serviceFacade.getLabel(groupId, id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);
        entity.setLabel(populateRemainingLabelContent(label));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified label.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the label to update.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @param width The width of the label.
     * @param height The height of the label.
     * @param label The label's value.
     * @param formParams Additionally, the label styles are specified in the
     * form parameters. They are specified in a map-like fashion:
     * <br>
     * <ul>
     * <li>style[background-color]=#aaaaaa</li>
     * </ul>
     *
     * @return A labelEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(LabelEntity.class)
    public Response updateLabel(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id, @FormParam("label") String label,
            @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y,
            @FormParam("width") DoubleParameter width, @FormParam("height") DoubleParameter height,
            MultivaluedMap<String, String> formParams) {

        final Map<String, String> labelStyle = new LinkedHashMap<>();

        // go through each parameter and look for processor properties
        for (String parameterName : formParams.keySet()) {
            if (StringUtils.isNotBlank(parameterName)) {
                // see if the parameter name starts with an expected parameter type...
                if (parameterName.startsWith("style")) {
                    final int startIndex = StringUtils.indexOf(parameterName, "[");
                    final int endIndex = StringUtils.lastIndexOf(parameterName, "]");
                    if (startIndex != -1 && endIndex != -1) {
                        final String styleName = StringUtils.substring(parameterName, startIndex + 1, endIndex);
                        labelStyle.put(styleName, formParams.getFirst(parameterName));
                    }
                }
            }
        }

        // create the label DTO
        final LabelDTO labelDTO = new LabelDTO();
        labelDTO.setId(id);
        labelDTO.setLabel(label);

        // only set the styles when appropriate
        if (!labelStyle.isEmpty()) {
            labelDTO.setStyle(labelStyle);
        }

        // require both coordinates to be specified
        if (x != null && y != null) {
            labelDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));
        }

        // require both width and height to be specified
        if (width != null && height != null) {
            labelDTO.setWidth(width.getDouble());
            labelDTO.setHeight(height.getDouble());
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the label entity
        final LabelEntity labelEntity = new LabelEntity();
        labelEntity.setRevision(revision);
        labelEntity.setLabel(labelDTO);

        // update the label
        return updateLabel(httpServletRequest, id, labelEntity);
    }

    /**
     * Updates the specified label.
     *
     * @param httpServletRequest
     * @param id The id of the label to update.
     * @param labelEntity A labelEntity.
     * @return A labelEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(LabelEntity.class)
    public Response updateLabel(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("id") String id,
            LabelEntity labelEntity) {

        if (labelEntity == null || labelEntity.getLabel() == null) {
            throw new IllegalArgumentException("Label details must be specified.");
        }

        if (labelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final LabelDTO requestLabelDTO = labelEntity.getLabel();
        if (!id.equals(requestLabelDTO.getId())) {
            throw new IllegalArgumentException(String.format("The label id (%s) in the request body does not equal the "
                    + "label id of the requested resource (%s).", requestLabelDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(labelEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // update the label
        final RevisionDTO revision = labelEntity.getRevision();
        final ConfigurationSnapshot<LabelDTO> controllerResponse = serviceFacade.updateLabel(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, requestLabelDTO);

        // get the results
        final LabelDTO responseLabelDTO = controllerResponse.getConfiguration();
        populateRemainingLabelContent(responseLabelDTO);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final LabelEntity entity = new LabelEntity();
        entity.setRevision(updatedRevision);
        entity.setLabel(responseLabelDTO);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Removes the specified label.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the label to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(LabelEntity.class)
    public Response removeLabel(
            @Context HttpServletRequest httpServletRequest,
            @QueryParam(VERSION) LongParameter version,
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
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

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the specified label
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteLabel(new Revision(clientVersion, clientId.getClientId()), groupId, id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);

        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
