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

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.apache.nifi.web.util.WebUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.representation.Form;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.server.impl.model.method.dispatch.FormDispatchProvider;

/**
 * Base class for controllers.
 */
public abstract class ApplicationResource {

    public static final String VERSION = "version";
    public static final String CLIENT_ID = "clientId";
    public static final String CLUSTER_CONTEXT_HTTP_HEADER = "X-ClusterContext";
    public static final String PROXY_SCHEME_HTTP_HEADER = "X-ProxyScheme";
    public static final String PROXY_HOST_HTTP_HEADER = "X-ProxyHost";
    public static final String PROXY_PORT_HTTP_HEADER = "X-ProxyPort";
    public static final String PROXY_CONTEXT_PATH_HTTP_HEADER = "X-ProxyContextPath";
    public static final String PROXIED_ENTITIES_CHAIN_HTTP_HEADER = "X-ProxiedEntitiesChain";
    public static final String PROXIED_ENTITY_USER_DETAILS_HTTP_HEADER = "X-ProxiedEntityUserDetails";

    private static final Logger logger = LoggerFactory.getLogger(ApplicationResource.class);

    public static final String NODEWISE = "false";

    @Context
    private HttpServletRequest httpServletRequest;

    @Context
    private UriInfo uriInfo;

    @Context
    private HttpContext httpContext;

    /**
     * Generate a resource uri based off of the specified parameters.
     *
     * @param path path
     * @return resource uri
     */
    protected String generateResourceUri(String... path) {
        UriBuilder uriBuilder = uriInfo.getBaseUriBuilder();
        uriBuilder.segment(path);
        URI uri = uriBuilder.build();
        try {

            // check for proxy settings
            String scheme = httpServletRequest.getHeader(PROXY_SCHEME_HTTP_HEADER);
            String host = httpServletRequest.getHeader(PROXY_HOST_HTTP_HEADER);
            String port = httpServletRequest.getHeader(PROXY_PORT_HTTP_HEADER);
            String baseContextPath = httpServletRequest.getHeader(PROXY_CONTEXT_PATH_HTTP_HEADER);

            // if necessary, prepend the context path
            String resourcePath = uri.getPath();
            if (baseContextPath != null) {
                // normalize context path
                if (!baseContextPath.startsWith("/")) {
                    baseContextPath = "/" + baseContextPath;
                }

                // determine the complete resource path
                resourcePath = baseContextPath + resourcePath;
            }

            // determine the port uri
            int uriPort = uri.getPort();
            if (port != null) {
                if (StringUtils.isWhitespace(port)) {
                    uriPort = -1;
                } else {
                    try {
                        uriPort = Integer.parseInt(port);
                    } catch (NumberFormatException nfe) {
                        logger.warn(String.format("Unable to parse proxy port HTTP header '%s'. Using port from request URI '%s'.", port, uriPort));
                    }
                }
            }

            // construct the URI
            uri = new URI(
                    (StringUtils.isBlank(scheme)) ? uri.getScheme() : scheme,
                    uri.getUserInfo(),
                    (StringUtils.isBlank(host)) ? uri.getHost() : host,
                    uriPort,
                    resourcePath,
                    uri.getQuery(),
                    uri.getFragment());

        } catch (final URISyntaxException use) {
            throw new UriBuilderException(use);
        }
        return uri.toString();
    }

    /**
     * Edit the response headers to indicating no caching.
     *
     * @param response response
     * @return builder
     */
    protected ResponseBuilder noCache(ResponseBuilder response) {
        CacheControl cacheControl = new CacheControl();
        cacheControl.setPrivate(true);
        cacheControl.setNoCache(true);
        cacheControl.setNoStore(true);
        return response.cacheControl(cacheControl);
    }

    /**
     * If the application is operating as a node, then this method adds the cluster context information to the response using the response header 'X-CLUSTER_CONTEXT'.
     *
     * @param response response
     * @return builder
     */
    protected ResponseBuilder clusterContext(ResponseBuilder response) {
        // TODO: Remove this method. Since ClusterContext was removed, it is no longer needed. However,
        // it is called by practically every endpoint so for now it is just being stubbed out.
        return response;
    }

    protected String generateUuid() {
        final Optional<String> seed = getIdGenerationSeed();
        return seed.isPresent() ? UUID.nameUUIDFromBytes(seed.get().getBytes(StandardCharsets.UTF_8)).toString() : UUID.randomUUID().toString();
    }

    protected Optional<String> getIdGenerationSeed() {
        final String idGenerationSeed = httpServletRequest.getHeader(RequestReplicator.CLUSTER_ID_GENERATION_SEED_HEADER);
        if (StringUtils.isBlank(idGenerationSeed)) {
            return Optional.empty();
        }

        return Optional.of(idGenerationSeed);
    }


    /**
     * Generates an Ok response with no content.
     *
     * @return an Ok response with no content
     */
    protected ResponseBuilder generateOkResponse() {
        return noCache(Response.ok());
    }

    /**
     * Generates an Ok response with the specified content.
     *
     * @param entity The entity
     * @return The response to be built
     */
    protected ResponseBuilder generateOkResponse(Object entity) {
        ResponseBuilder response = Response.ok(entity);
        return noCache(response);
    }

    /**
     * Generates a 201 Created response with the specified content.
     *
     * @param uri The URI
     * @param entity entity
     * @return The response to be built
     */
    protected ResponseBuilder generateCreatedResponse(URI uri, Object entity) {
        // generate the response builder
        return Response.created(uri).entity(entity);
    }

    /**
     * Generates a 401 Not Authorized response with no content.

     * @return The response to be built
     */
    protected ResponseBuilder generateNotAuthorizedResponse() {
        // generate the response builder
        return Response.status(HttpServletResponse.SC_UNAUTHORIZED);
    }

    /**
     * Generates a 150 Node Continue response to be used within the cluster request handshake.
     *
     * @return a 150 Node Continue response to be used within the cluster request handshake
     */
    protected ResponseBuilder generateContinueResponse() {
        return Response.status(RequestReplicator.NODE_CONTINUE_STATUS_CODE);
    }

    protected URI getAbsolutePath() {
        return uriInfo.getAbsolutePath();
    }

    protected MultivaluedMap<String, String> getRequestParameters() {
        final MultivaluedMap<String, String> entity = new MultivaluedMapImpl();

        // get the form that jersey processed and use it if it exists (only exist for requests with a body and application form urlencoded
        final Form form = (Form) httpContext.getProperties().get(FormDispatchProvider.FORM_PROPERTY);
        if (form == null) {
            for (Map.Entry<String, String[]> entry : httpServletRequest.getParameterMap().entrySet()) {
                if (entry.getValue() == null) {
                    entity.add(entry.getKey(), null);
                } else {
                    for (final String aValue : entry.getValue()) {
                        entity.add(entry.getKey(), aValue);
                    }
                }
            }
        } else {
            entity.putAll(form);
        }

        return entity;
    }

    protected MultivaluedMap<String, String> getRequestParameters(final boolean forceClientId) {
        final MultivaluedMap<String, String> params = getRequestParameters();
        if (forceClientId) {
            if (StringUtils.isBlank(params.getFirst(CLIENT_ID))) {
                params.putSingle(CLIENT_ID, new ClientIdParameter().getClientId());
            }
        }
        return params;
    }

    protected Map<String, String> getHeaders() {
        return getHeaders(new HashMap<String, String>());
    }

    protected Map<String, String> getHeaders(final Map<String, String> overriddenHeaders) {

        final Map<String, String> result = new HashMap<>();
        final Map<String, String> overriddenHeadersIgnoreCaseMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        overriddenHeadersIgnoreCaseMap.putAll(overriddenHeaders);

        final Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
        while (headerNames.hasMoreElements()) {

            final String headerName = headerNames.nextElement();
            if (!overriddenHeadersIgnoreCaseMap.isEmpty() && headerName.equalsIgnoreCase("content-length")) {
                continue;
            }
            if (overriddenHeadersIgnoreCaseMap.containsKey(headerName)) {
                result.put(headerName, overriddenHeadersIgnoreCaseMap.get(headerName));
            } else {
                result.put(headerName, httpServletRequest.getHeader(headerName));
            }
        }

        // set the proxy scheme to request scheme if not already set client
        String proxyScheme = httpServletRequest.getHeader(PROXY_SCHEME_HTTP_HEADER);
        if (proxyScheme == null) {
            result.put(PROXY_SCHEME_HTTP_HEADER, httpServletRequest.getScheme());
        }

        if (httpServletRequest.isSecure()) {

            // add the user's authorities (if any) to the headers
            final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if (authentication != null) {
                final Object userDetailsObj = authentication.getPrincipal();
                if (userDetailsObj instanceof NiFiUserDetails) {
                    // serialize user details object
                    final String hexEncodedUserDetails = WebUtils.serializeObjectToHex((Serializable) userDetailsObj);

                    // put serialized user details in header
                    result.put(PROXIED_ENTITY_USER_DETAILS_HTTP_HEADER, hexEncodedUserDetails);

                    // remove the access token if present, since the user is already authenticated/authorized
                    result.remove(JwtAuthenticationFilter.AUTHORIZATION);
                }
            }
        }
        return result;
    }

    /**
     * Checks whether the request is part of a two-phase commit style request (either phase 1 or phase 2)
     *
     * @param httpServletRequest the request
     * @return <code>true</code> if the request represents a two-phase commit style request
     */
    protected boolean isTwoPhaseRequest(HttpServletRequest httpServletRequest) {
        final String headerValue = httpServletRequest.getHeader(RequestReplicator.REQUEST_TRANSACTION_ID_HEADER);
        return headerValue != null;
    }

    /**
     * When a two-phase commit style request is used, the first phase (generally referred to
     * as the "commit-request stage") is intended to validate that the request can be completed.
     * In NiFi, we use this phase to validate that the request can complete. This method determines
     * whether or not the request is the first phase of a two-phase commit.
     *
     * @param httpServletRequest the request
     * @return <code>true</code> if the request represents a two-phase commit style request and is the
     *         first of the two phases.
     */
    protected boolean isValidationPhase(HttpServletRequest httpServletRequest) {
        return isTwoPhaseRequest(httpServletRequest) && httpServletRequest.getHeader(RequestReplicator.REQUEST_VALIDATION_HTTP_HEADER) != null;
    }

    /**
     * Converts a Revision DTO and an associated Component ID into a Revision object
     *
     * @param revisionDto the Revision DTO
     * @param componentId the ID of the component that the Revision DTO belongs to
     * @return a Revision that has the same client ID and Version as the Revision DTO and the Component ID specified
     */
    protected Revision getRevision(RevisionDTO revisionDto, String componentId) {
        return new Revision(revisionDto.getVersion(), revisionDto.getClientId(), componentId);
    }

    /**
     * Extracts a Revision object from the Revision DTO and ID provided by the Component Entity
     *
     * @param entity the ComponentEntity that contains the Revision DTO & ID
     * @return the Revision specified in the ComponentEntity
     */
    protected Revision getRevision(ComponentEntity entity, String componentId) {
        return getRevision(entity.getRevision(), componentId);
    }
}
