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
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeWorkload;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.client.http.TransportProtocolVersionNegotiator;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * RESTful endpoint for managing a SiteToSite connection.
 */
@Path("/site-to-site")
@Api(
        value = "/site-to-site",
        description = "Provide access to site to site with this NiFi"
)
public class SiteToSiteResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteResource.class);

    private NiFiServiceFacade serviceFacade;
    private ClusterCoordinator clusterCoordinator;
    private Authorizer authorizer;

    private final ResponseCreator responseCreator = new ResponseCreator();
    private final VersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);
    private final HttpRemoteSiteListener transactionManager;

    public SiteToSiteResource(final NiFiProperties nifiProperties) {
        transactionManager = HttpRemoteSiteListener.getInstance(nifiProperties);
    }

    /**
     * Authorizes access to Site To Site details.
     * <p>
     * Note: Protected for testing purposes
     */
    protected void authorizeSiteToSite() {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable siteToSite = lookup.getSiteToSite();
            siteToSite.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Returns the details of this NiFi.
     *
     * @return A controllerEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Returns the details about this NiFi necessary to communicate via site to site",
            response = ControllerEntity.class,
            authorizations = {
                @Authorization(value = "Read - /site-to-site", type = "")
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
    public Response getSiteToSiteDetails(@Context HttpServletRequest req) {

        authorizeSiteToSite();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the controller dto
        final ControllerDTO controller = serviceFacade.getSiteToSiteDetails();

        // build the response entity
        final ControllerEntity entity = new ControllerEntity();
        entity.setController(controller);

        if (isEmpty(req.getHeader(HttpHeaders.PROTOCOL_VERSION))) {
            // This indicates the client uses older NiFi version,
            // which strictly read JSON properties and fail with unknown properties.
            // Convert result entity so that old version clients can understand.
            logger.debug("Converting result to provide backward compatibility...");
            controller.setRemoteSiteHttpListeningPort(null);
        }

        // generate the response
        return noCache(Response.ok(entity)).build();
    }

    /**
     * Returns the available Peers and its status of this NiFi.
     *
     * @return A peersEntity.
     */
    @GET
    @Path("/peers")
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @ApiOperation(
            value = "Returns the available Peers and its status of this NiFi",
            response = PeersEntity.class,
            authorizations = {
                @Authorization(value = "Read - /site-to-site", type = "")
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
    public Response getPeers(@Context HttpServletRequest req) {

        authorizeSiteToSite();

        if (!properties.isSiteToSiteHttpEnabled()) {
            return responseCreator.httpSiteToSiteIsNotEnabledResponse();
        }

        final Integer transportProtocolVersion;
        try {
            transportProtocolVersion = negotiateTransportProtocolVersion(req, transportProtocolVersionNegotiator);
        } catch (BadRequestException e) {
            return responseCreator.badRequestResponse(e);
        }

        final List<PeerDTO> peers = new ArrayList<>();
        if (properties.isNode()) {

            try {
                final Map<NodeIdentifier, NodeWorkload> clusterWorkload = clusterCoordinator.getClusterWorkload();
                clusterWorkload.entrySet().stream().forEach(entry -> {
                    final PeerDTO peer = new PeerDTO();
                    final NodeIdentifier nodeId = entry.getKey();
                    final String siteToSiteAddress = nodeId.getSiteToSiteAddress();
                    peer.setHostname(siteToSiteAddress == null ? nodeId.getApiAddress() : siteToSiteAddress);
                    peer.setPort(nodeId.getSiteToSiteHttpApiPort() == null ? nodeId.getApiPort() : nodeId.getSiteToSiteHttpApiPort());
                    peer.setSecure(nodeId.isSiteToSiteSecure());
                    peer.setFlowFileCount(entry.getValue().getFlowFileCount());
                    peers.add(peer);
                });
            } catch (IOException e) {
                throw new RuntimeException("Failed to retrieve cluster workload due to " + e, e);
            }

        } else {
            // Standalone mode.
            final PeerDTO peer = new PeerDTO();

            // Private IP address or hostname may not be accessible from client in some environments.
            // So, use the value defined in nifi.properties instead when it is defined.
            final String remoteInputHost = properties.getRemoteInputHost();
            String localName;
            try {
                // Get local host name using InetAddress if available, same as RAW socket does.
                localName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to get local host name using InetAddress.", e);
                }
                localName = req.getLocalName();
            }

            peer.setHostname(isEmpty(remoteInputHost) ? localName : remoteInputHost);
            peer.setPort(properties.getRemoteInputHttpPort());
            peer.setSecure(properties.isSiteToSiteSecure());
            peer.setFlowFileCount(0);  // doesn't matter how many FlowFiles we have, because we're the only host.

            peers.add(peer);
        }

        final PeersEntity entity = new PeersEntity();
        entity.setPeers(peers);

        return noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion, transactionManager)).build();
    }

    // setters

    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        super.setClusterCoordinator(clusterCoordinator);
        this.clusterCoordinator = clusterCoordinator;
    }
}
