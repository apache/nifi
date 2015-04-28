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
import javax.ws.rs.core.Response;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.FunnelsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DoubleParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Funnel.
 */
public class FunnelResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(FunnelResource.class);

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
    private String groupId;

    /**
     * Populates the uri for the specified funnels.
     *
     * @param funnels funnels
     * @return funnels
     */
    public Set<FunnelDTO> populateRemainingFunnelsContent(Set<FunnelDTO> funnels) {
        for (FunnelDTO funnel : funnels) {
            populateRemainingFunnelContent(funnel);
        }
        return funnels;
    }

    /**
     * Populates the uri for the specified funnel.
     */
    private FunnelDTO populateRemainingFunnelContent(FunnelDTO funnel) {
        // populate the funnel href
        funnel.setUri(generateResourceUri("controller", "process-groups", groupId, "funnels", funnel.getId()));
        return funnel;
    }

    /**
     * Retrieves all the of funnels in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A funnelsEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(FunnelsEntity.class)
    public Response getFunnels(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the funnels
        final Set<FunnelDTO> funnels = populateRemainingFunnelsContent(serviceFacade.getFunnels(groupId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final FunnelsEntity entity = new FunnelsEntity();
        entity.setRevision(revision);
        entity.setFunnels(funnels);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new funnel.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @return A funnelEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(FunnelEntity.class)
    public Response createFunnel(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y) {

        // ensure the position has been specified
        if (x == null || y == null) {
            throw new IllegalArgumentException("The position (x, y) must be specified");
        }

        // create the funnel DTO
        final FunnelDTO funnelDTO = new FunnelDTO();
        funnelDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the funnel entity
        final FunnelEntity funnelEntity = new FunnelEntity();
        funnelEntity.setRevision(revision);
        funnelEntity.setFunnel(funnelDTO);

        return createFunnel(httpServletRequest, funnelEntity);
    }

    /**
     * Creates a new Funnel.
     *
     * @param httpServletRequest request
     * @param funnelEntity A funnelEntity.
     * @return A funnelEntity.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(FunnelEntity.class)
    public Response createFunnel(
            @Context HttpServletRequest httpServletRequest,
            FunnelEntity funnelEntity) {

        if (funnelEntity == null || funnelEntity.getFunnel() == null) {
            throw new IllegalArgumentException("Funnel details must be specified.");
        }

        if (funnelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (funnelEntity.getFunnel().getId() != null) {
            throw new IllegalArgumentException("Funnel ID cannot be specified.");
        }

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager()) {

            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            funnelEntity.getFunnel().setId(id);

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
            return (Response) clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(funnelEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the funnel and generate the json
        final RevisionDTO revision = funnelEntity.getRevision();
        final ConfigurationSnapshot<FunnelDTO> controllerResponse = serviceFacade.createFunnel(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, funnelEntity.getFunnel());
        final FunnelDTO funnel = controllerResponse.getConfiguration();
        populateRemainingFunnelContent(funnel);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final FunnelEntity entity = new FunnelEntity();
        entity.setRevision(updatedRevision);
        entity.setFunnel(funnel);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(funnel.getUri()), entity)).build();
    }

    /**
     * Retrieves the specified funnel.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the funnel to retrieve
     * @return A funnelEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(FunnelEntity.class)
    public Response getFunnel(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId, @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the funnel
        final FunnelDTO funnel = serviceFacade.getFunnel(groupId, id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final FunnelEntity entity = new FunnelEntity();
        entity.setRevision(revision);
        entity.setFunnel(populateRemainingFunnelContent(funnel));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified funnel.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the funnel to update.
     * @param parentGroupId The id of the process group to move this funnel to.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @return A funnelEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(FunnelEntity.class)
    public Response updateFunnel(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id, @FormParam("parentGroupId") String parentGroupId,
            @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y) {

        // create the funnel DTO
        final FunnelDTO funnelDTO = new FunnelDTO();
        funnelDTO.setId(id);
        funnelDTO.setParentGroupId(parentGroupId);

        // require both coordinates to be specified
        if (x != null && y != null) {
            funnelDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the funnel entity
        final FunnelEntity funnelEntity = new FunnelEntity();
        funnelEntity.setRevision(revision);
        funnelEntity.setFunnel(funnelDTO);

        // update the funnel
        return updateFunnel(httpServletRequest, id, funnelEntity);
    }

    /**
     * Creates a new Funnel.
     *
     * @param httpServletRequest request
     * @param id The id of the funnel to update.
     * @param funnelEntity A funnelEntity.
     * @return A funnelEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(FunnelEntity.class)
    public Response updateFunnel(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("id") String id,
            FunnelEntity funnelEntity) {

        if (funnelEntity == null || funnelEntity.getFunnel() == null) {
            throw new IllegalArgumentException("Funnel details must be specified.");
        }

        if (funnelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final FunnelDTO requestFunnelDTO = funnelEntity.getFunnel();
        if (!id.equals(requestFunnelDTO.getId())) {
            throw new IllegalArgumentException(String.format("The funnel id (%s) in the request body does not equal the "
                    + "funnel id of the requested resource (%s).", requestFunnelDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(funnelEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // update the funnel
        final RevisionDTO revision = funnelEntity.getRevision();
        final ConfigurationSnapshot<FunnelDTO> controllerResponse = serviceFacade.updateFunnel(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, requestFunnelDTO);

        // get the results
        final FunnelDTO responseFunnelDTO = controllerResponse.getConfiguration();
        populateRemainingFunnelContent(responseFunnelDTO);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final FunnelEntity entity = new FunnelEntity();
        entity.setRevision(updatedRevision);
        entity.setFunnel(responseFunnelDTO);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Removes the specified funnel.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the funnel to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(FunnelEntity.class)
    public Response removeFunnel(
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
            serviceFacade.verifyDeleteFunnel(groupId, id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the specified funnel
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteFunnel(new Revision(clientVersion, clientId.getClientId()), groupId, id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final FunnelEntity entity = new FunnelEntity();
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
