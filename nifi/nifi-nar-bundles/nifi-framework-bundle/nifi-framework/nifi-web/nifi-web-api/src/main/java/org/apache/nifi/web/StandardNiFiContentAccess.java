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
package org.apache.nifi.web;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.user.NiFiUserDetails;
import org.apache.nifi.web.util.WebUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 *
 */
public class StandardNiFiContentAccess implements ContentAccess {

    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiContentAccess.class);
    public static final String CLIENT_ID_PARAM = "clientId";

    private NiFiProperties properties;
    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;

    @Override
    @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    public DownloadableContent getContent(final ContentRequestContext request) {
        // if clustered, send request to cluster manager
        if (properties.isClusterManager()) {
            // get the URI
            URI dataUri;
            try {
                dataUri = new URI(request.getDataUri());
            } catch (final URISyntaxException use) {
                throw new ClusterRequestException(use);
            }

            // set the request parameters
            final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();
            parameters.add(CLIENT_ID_PARAM, request.getClientId());

            // set the headers
            final Map<String, String> headers = new HashMap<>();
            if (StringUtils.isNotBlank(request.getProxiedEntitiesChain())) {
                headers.put("X-ProxiedEntitiesChain", request.getProxiedEntitiesChain());
            }

            // add the user's authorities (if any) to the headers
            final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if (authentication != null) {
                final Object userDetailsObj = authentication.getPrincipal();
                if (userDetailsObj instanceof NiFiUserDetails) {
                    // serialize user details object
                    final String hexEncodedUserDetails = WebUtils.serializeObjectToHex((Serializable) userDetailsObj);

                    // put serialized user details in header
                    headers.put("X-ProxiedEntityUserDetails", hexEncodedUserDetails);
                }
            }

            // get the target node and ensure it exists
            final Node targetNode = clusterManager.getNode(request.getClusterNodeId());
            if (targetNode == null) {
                throw new UnknownNodeException("The specified cluster node does not exist.");
            }

            final Set<NodeIdentifier> targetNodes = new HashSet<>();
            targetNodes.add(targetNode.getNodeId());

            // replicate the request to the specific node
            final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, dataUri, parameters, headers, targetNodes);
            final ClientResponse clientResponse = nodeResponse.getClientResponse();
            final MultivaluedMap<String, String> responseHeaders = clientResponse.getHeaders();

            // get the file name
            final String contentDisposition = responseHeaders.getFirst("Content-Disposition");
            final String filename = StringUtils.substringAfterLast(contentDisposition, "filename=");

            // get the content type
            final String contentType = responseHeaders.getFirst("Content-Type");

            // create the downloadable content
            return new DownloadableContent(filename, contentType, clientResponse.getEntityInputStream());
        } else {
            // example URI: http://localhost:8080/nifi-api/controller/provenance/events/1/content/input
            final String eventDetails = StringUtils.substringAfterLast(request.getDataUri(), "events/");
            final String rawEventId = StringUtils.substringBefore(eventDetails, "/content/");
            final String rawDirection = StringUtils.substringAfterLast(eventDetails, "/content/");

            // get the content type
            final Long eventId;
            final ContentDirection direction;
            try {
                eventId = Long.parseLong(rawEventId);
                direction = ContentDirection.valueOf(rawDirection.toUpperCase());
            } catch (final IllegalArgumentException iae) {
                throw new IllegalArgumentException("The specified data reference URI is not valid.");
            }
            return serviceFacade.getContent(eventId, request.getDataUri(), direction);
        }
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }
}
