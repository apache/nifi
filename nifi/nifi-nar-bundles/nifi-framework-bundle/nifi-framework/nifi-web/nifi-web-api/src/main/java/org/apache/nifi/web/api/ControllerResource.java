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

import com.sun.jersey.api.core.ResourceContext;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
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
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AboutDTO;
import org.apache.nifi.web.api.dto.BannerDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.entity.AboutEntity;
import org.apache.nifi.web.api.entity.AuthorityEntity;
import org.apache.nifi.web.api.entity.BannerEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.ControllerStatusEntity;
import org.apache.nifi.web.api.entity.CounterEntity;
import org.apache.nifi.web.api.entity.CountersEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.PrioritizerTypesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.apache.nifi.web.api.entity.SearchResultsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.api.entity.ControllerServiceTypesEntity;
import org.apache.nifi.web.api.entity.ReportingTaskTypesEntity;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Flow Controller.
 */
@Path("/controller")
public class ControllerResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    @Context
    private ResourceContext resourceContext;

    /**
     * Locates the Provenance sub-resource.
     *
     * @return
     */
    @Path("/provenance")
    public ProvenanceResource getProvenanceResource() {
        return resourceContext.getResource(ProvenanceResource.class);
    }

    /**
     * Locates the User sub-resource.
     *
     * @return
     */
    @Path("/users")
    public UserResource getUserResource() {
        return resourceContext.getResource(UserResource.class);
    }

    /**
     * Locates the User sub-resource.
     *
     * @return
     */
    @Path("/user-groups")
    public UserGroupResource getUserGroupResource() {
        return resourceContext.getResource(UserGroupResource.class);
    }

    /**
     * Locates the History sub-resource.
     *
     * @return
     */
    @Path("/history")
    public HistoryResource getHistoryResource() {
        return resourceContext.getResource(HistoryResource.class);
    }

    /**
     * Locates the History sub-resource.
     *
     * @return
     */
    @Path("/bulletin-board")
    public BulletinBoardResource getBulletinBoardResource() {
        return resourceContext.getResource(BulletinBoardResource.class);
    }

    /**
     * Locates the Template sub-resource.
     *
     * @return
     */
    @Path("/templates")
    public TemplateResource getTemplateResource() {
        return resourceContext.getResource(TemplateResource.class);
    }

    /**
     * Locates the Snippets sub-resource.
     *
     * @return
     */
    @Path("/snippets")
    public SnippetResource getSnippetResource() {
        return resourceContext.getResource(SnippetResource.class);
    }
    
    /**
     * Locates the Controller Services sub-resource.
     *
     * @return
     */
    @Path("/controller-services")
    public ControllerServiceResource getControllerServiceResource() {
        return resourceContext.getResource(ControllerServiceResource.class);
    }
    
    /**
     * Locates the Reporting Tasks sub-resource.
     *
     * @return
     */
    @Path("/reporting-tasks")
    public ReportingTaskResource getReportingTaskResource() {
        return resourceContext.getResource(ReportingTaskResource.class);
    }

    /**
     * Locates the Group sub-resource.
     *
     * @param groupId The process group id
     * @return
     */
    @Path("/process-groups/{process-group-id}")
    public ProcessGroupResource getGroupResource(@PathParam("process-group-id") String groupId) {
        ProcessGroupResource groupResource = resourceContext.getResource(ProcessGroupResource.class);
        groupResource.setGroupId(groupId);
        return groupResource;
    }

    /**
     * Returns a 200 OK response to indicate this is a valid controller
     * endpoint.
     *
     * @return An OK response with an empty entity body.
     */
    @HEAD
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response getControllerHead() {
        if (properties.isClusterManager()) {
            throw new IllegalClusterResourceRequestException("A cluster manager cannot process the request.");
        }

        return Response.ok().build();
    }

    /**
     * Returns the details of this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A controllerEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_NIFI')")
    @TypeHint(ControllerEntity.class)
    public Response getController(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller dto
        final ControllerDTO controller = serviceFacade.getController();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // build the response entity
        final ControllerEntity entity = new ControllerEntity();
        entity.setRevision(revision);
        entity.setController(controller);

        // generate the response
        return clusterContext(noCache(Response.ok(entity))).build();
    }

    /**
     * Performs a search request for this controller.
     *
     * @param value Search string
     * @return A searchResultsEntity
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/search-results")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(SearchResultsEntity.class)
    public Response searchController(@QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {
        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // query the controller
        final SearchResultsDTO results = serviceFacade.searchController(value);

        // create the entity
        final SearchResultsEntity entity = new SearchResultsEntity();
        entity.setSearchResultsDTO(results);

        // generate the response
        return clusterContext(noCache(Response.ok(entity))).build();
    }

    /**
     * Creates a new archive of this flow controller. Note, this is a POST
     * operation that returns a URI that is not representative of the thing that
     * was actually created. The archive that is created cannot be referenced at
     * a later time, therefore there is no corresponding URI. Instead the
     * request URI is returned.
     *
     * Alternatively, we could have performed a PUT request. However, PUT
     * requests are supposed to be idempotent and this endpoint is certainly
     * not.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A processGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/archive")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ProcessGroupEntity.class)
    public Response createArchive(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // don't need to convert from POST to PUT because no resource ID exists on the nodes
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
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

        // create the archive
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.createArchive(new Revision(clientVersion, clientId.getClientId()));

        // create the revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(clientId.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ProcessGroupEntity controllerEntity = new ProcessGroupEntity();
        controllerEntity.setRevision(updatedRevision);

        // generate the response
        URI uri = URI.create(generateResourceUri("controller", "archive"));
        return clusterContext(generateCreatedResponse(uri, controllerEntity)).build();
    }

    /**
     * Gets current revision of this NiFi.
     *
     * @return A revisionEntity
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/revision")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(Entity.class)
    public Response getRevision() {
        // create the current revision
        final RevisionDTO revision = serviceFacade.getRevision();

        // create the response entity
        final Entity entity = new Entity();
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the status for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A controllerStatusEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/status")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ControllerStatusEntity.class)
    public Response getControllerStatus(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        final ControllerStatusDTO controllerStatus = serviceFacade.getControllerStatus();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ControllerStatusEntity entity = new ControllerStatusEntity();
        entity.setRevision(revision);
        entity.setControllerStatus(controllerStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the counters report for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A countersEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/counters")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(CountersEntity.class)
    public Response getCounters(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        final CountersDTO countersReport = serviceFacade.getCounters();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final CountersEntity entity = new CountersEntity();
        entity.setRevision(revision);
        entity.setCounters(countersReport);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Update the specified counter. This will reset the counter value to 0.
     *
     * @param httpServletRequest
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the counter.
     * @return A counterEntity.
     */
    @PUT
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/counters/{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(CounterEntity.class)
    public Response updateCounter(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // reset the specified counter
        final CounterDTO counter = serviceFacade.updateCounter(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final CounterEntity entity = new CounterEntity();
        entity.setRevision(revision);
        entity.setCounter(counter);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the configuration for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A controllerConfigurationEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/config")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_NIFI')")
    @TypeHint(ControllerConfigurationEntity.class)
    public Response getControllerConfig(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {
        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        final ControllerConfigurationDTO controllerConfig = serviceFacade.getControllerConfiguration();
        controllerConfig.setUri(generateResourceUri("controller"));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(revision);
        entity.setConfig(controllerConfig);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Update the configuration for this NiFi.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param name The name of this controller.
     * @param comments The comments of this controller.
     * @param maxTimerDrivenThreadCount The maximum number of timer driven
     * threads this controller has available.
     * @param maxEventDrivenThreadCount The maximum number of timer driven
     * threads this controller has available.
     * @return A controllerConfigurationEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/config")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ControllerConfigurationEntity.class)
    public Response updateControllerConfig(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("name") String name,
            @FormParam("comments") String comments,
            @FormParam("maxTimerDrivenThreadCount") IntegerParameter maxTimerDrivenThreadCount,
            @FormParam("maxEventDrivenThreadCount") IntegerParameter maxEventDrivenThreadCount) {

        // create the controller config dto
        final ControllerConfigurationDTO configDTO = new ControllerConfigurationDTO();
        configDTO.setName(name);
        configDTO.setComments(comments);

        if (maxTimerDrivenThreadCount != null) {
            configDTO.setMaxTimerDrivenThreadCount(maxTimerDrivenThreadCount.getInteger());
        }

        if (maxEventDrivenThreadCount != null) {
            configDTO.setMaxEventDrivenThreadCount(maxEventDrivenThreadCount.getInteger());
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the dto entity
        ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(revision);
        entity.setConfig(configDTO);

        // update the controller configuration
        return updateControllerConfig(httpServletRequest, entity);
    }

    /**
     * Update the configuration for this NiFi.
     *
     * @param httpServletRequest
     * @param configEntity A controllerConfigurationEntity.
     * @return A controllerConfigurationEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/config")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ControllerConfigurationEntity.class)
    public Response updateControllerConfig(
            @Context HttpServletRequest httpServletRequest,
            ControllerConfigurationEntity configEntity) {

        if (configEntity == null || configEntity.getConfig() == null) {
            throw new IllegalArgumentException("Controller configuration must be specified");
        }

        if (configEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(configEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        final RevisionDTO revision = configEntity.getRevision();
        final ConfigurationSnapshot<ControllerConfigurationDTO> controllerResponse
                = serviceFacade.updateControllerConfiguration(new Revision(revision.getVersion(), revision.getClientId()), configEntity.getConfig());
        final ControllerConfigurationDTO controllerConfig = controllerResponse.getConfiguration();
        controllerConfig.setUri(generateResourceUri("controller"));

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(updatedRevision);
        entity.setConfig(controllerConfig);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the user details, including the authorities, about the user
     * making the request.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A authoritiesEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/authorities")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(AuthorityEntity.class)
    public Response getAuthorities(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {
        // note that the cluster manager will handle this request directly
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        AuthorityEntity entity = new AuthorityEntity();
        entity.setRevision(revision);
        entity.setUserId(user.getId());
        entity.setAuthorities(NiFiUserUtils.getAuthorities());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the banners for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A bannerEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/banners")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(BannerEntity.class)
    public Response getBanners(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        final String bannerText = properties.getBannerText();

        // create the DTO
        final BannerDTO bannerDTO = new BannerDTO();
        bannerDTO.setHeaderText(bannerText);
        bannerDTO.setFooterText(bannerText);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final BannerEntity entity = new BannerEntity();
        entity.setRevision(revision);
        entity.setBanners(bannerDTO);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the types of processors that this NiFi supports.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A processorTypesEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/processor-types")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ProcessorTypesEntity.class)
    public Response getProcessorTypes(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create response entity
        final ProcessorTypesEntity entity = new ProcessorTypesEntity();
        entity.setRevision(revision);
        entity.setProcessorTypes(serviceFacade.getProcessorTypes());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }
    
    /**
     * Retrieves the types of controller services that this NiFi supports.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param serviceType Returns only services that implement this type
     * @return A controllerServicesTypesEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/controller-service-types")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ControllerServiceTypesEntity.class)
    public Response getControllerServiceTypes(
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @QueryParam("serviceType") String serviceType) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create response entity
        final ControllerServiceTypesEntity entity = new ControllerServiceTypesEntity();
        entity.setRevision(revision);
        entity.setControllerServiceTypes(serviceFacade.getControllerServiceTypes(serviceType));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }
    
    /**
     * Retrieves the types of reporting tasks that this NiFi supports.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A controllerServicesTypesEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/reporting-task-types")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ReportingTaskTypesEntity.class)
    public Response getReportingTaskTypes(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create response entity
        final ReportingTaskTypesEntity entity = new ReportingTaskTypesEntity();
        entity.setRevision(revision);
        entity.setReportingTaskTypes(serviceFacade.getReportingTaskTypes());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the types of prioritizers that this NiFi supports.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A prioritizerTypesEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/prioritizers")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(PrioritizerTypesEntity.class)
    public Response getPrioritizers(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create response entity
        final PrioritizerTypesEntity entity = new PrioritizerTypesEntity();
        entity.setRevision(revision);
        entity.setPrioritizerTypes(serviceFacade.getWorkQueuePrioritizerTypes());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves details about this NiFi to put in the About dialog.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return An aboutEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/about")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(AboutEntity.class)
    public Response getAboutInfo(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        final ControllerConfigurationDTO controllerConfig = serviceFacade.getControllerConfiguration();

        // create the about dto
        final AboutDTO aboutDTO = new AboutDTO();
        aboutDTO.setTitle(controllerConfig.getName());
        aboutDTO.setVersion(properties.getUiTitle());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final AboutEntity entity = new AboutEntity();
        entity.setRevision(revision);
        entity.setAbout(aboutDTO);

        // generate the response
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
