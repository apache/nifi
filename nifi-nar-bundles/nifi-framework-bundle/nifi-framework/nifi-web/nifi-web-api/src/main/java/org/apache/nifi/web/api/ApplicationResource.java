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

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.representation.Form;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.server.impl.model.method.dispatch.FormDispatchProvider;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;
import javax.ws.rs.core.UriInfo;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.user.NiFiUserDetails;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.util.WebUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

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

    private static final int HEADER_BUFFER_SIZE = 16 * 1024; // 16kb
    private static final int CLUSTER_CONTEXT_HEADER_VALUE_MAX_BYTES = (int) (0.75 * HEADER_BUFFER_SIZE);
    private static final Logger logger = LoggerFactory.getLogger(ApplicationResource.class);

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

        NiFiProperties properties = NiFiProperties.getInstance();
        if (!properties.isNode()) {
            return response;
        }

        // get cluster context from threadlocal
        ClusterContext clusterCtx = ClusterContextThreadLocal.getContext();
        if (clusterCtx != null) {

            // serialize cluster context
            String serializedClusterContext = WebUtils.serializeObjectToHex(clusterCtx);
            if (serializedClusterContext.length() > CLUSTER_CONTEXT_HEADER_VALUE_MAX_BYTES) {
                /*
                 * Actions is the only field that can vary in size. If we have no
                 * actions and we exceeded the header size, then basic assumptions
                 * about the cluster context have been violated.
                 */
                if (clusterCtx.getActions().isEmpty()) {
                    throw new IllegalStateException(
                            String.format("Serialized Cluster context size '%d' is too big for response header", serializedClusterContext.length()));
                }

                // use the first action as the prototype for creating the "batch" action
                Action prototypeAction = clusterCtx.getActions().get(0);

                // log the batched actions
                StringBuilder loggedActions = new StringBuilder();
                createBatchedActionLogStatement(loggedActions, clusterCtx.getActions());
                logger.info(loggedActions.toString());

                // remove current actions and replace with batch action
                clusterCtx.getActions().clear();

                // create the batch action
                FlowChangeAction batchAction = new FlowChangeAction();
                batchAction.setOperation(Operation.Batch);

                // copy values from prototype action
                batchAction.setTimestamp(prototypeAction.getTimestamp());
                batchAction.setUserIdentity(prototypeAction.getUserIdentity());
                batchAction.setUserName(prototypeAction.getUserName());
                batchAction.setSourceId(prototypeAction.getSourceId());
                batchAction.setSourceName(prototypeAction.getSourceName());
                batchAction.setSourceType(prototypeAction.getSourceType());

                // add batch action
                clusterCtx.getActions().add(batchAction);

                // create the final serialized copy of the cluster context
                serializedClusterContext = WebUtils.serializeObjectToHex(clusterCtx);
            }

            // put serialized cluster context in response header
            response.header(WebClusterManager.CLUSTER_CONTEXT_HTTP_HEADER, serializedClusterContext);
        }

        return response;
    }

    /**
     * @return the cluster context if found in the request header 'X-CLUSTER_CONTEXT'.
     */
    protected ClusterContext getClusterContextFromRequest() {
        String clusterContextHeaderValue = httpServletRequest.getHeader(WebClusterManager.CLUSTER_CONTEXT_HTTP_HEADER);
        if (StringUtils.isNotBlank(clusterContextHeaderValue)) {
            try {
                // deserialize object
                Serializable clusterContextObj = WebUtils.deserializeHexToObject(clusterContextHeaderValue);
                if (clusterContextObj instanceof ClusterContext) {
                    return (ClusterContext) clusterContextObj;
                }
            } catch (ClassNotFoundException cnfe) {
                logger.warn("Classpath issue detected because failed to deserialize cluster context from request due to: " + cnfe, cnfe);
            }
        }
        return null;
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
     * Generates a 150 Node Continue response to be used within the cluster request handshake.
     *
     * @return a 150 Node Continue response to be used within the cluster request handshake
     */
    protected ResponseBuilder generateContinueResponse() {
        return Response.status(WebClusterManager.NODE_CONTINUE_STATUS_CODE);
    }

    protected URI getAbsolutePath() {
        return uriInfo.getAbsolutePath();
    }

    protected MultivaluedMap<String, String> getRequestParameters() {
        final MultivaluedMap<String, String> entity = new MultivaluedMapImpl();

        // get the form that jersey processed and use it if it exists (only exist for requests with a body and application form urlencoded
        final Form form = (Form) httpContext.getProperties().get(FormDispatchProvider.FORM_PROPERTY);
        if (form == null) {
            for (Map.Entry<String, String[]> entry : (Set<Map.Entry<String, String[]>>) httpServletRequest.getParameterMap().entrySet()) {
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

    protected Entity updateClientId(final Entity entity) {
        if (entity != null && entity.getRevision() != null && StringUtils.isBlank(entity.getRevision().getClientId())) {
            entity.getRevision().setClientId(new ClientIdParameter().getClientId());
        }
        return entity;
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

            // add the certificate DN to the proxy chain
            final NiFiUser user = NiFiUserUtils.getNiFiUser();
            if (user != null) {
                // add the proxied user details
                result.put(PROXIED_ENTITIES_CHAIN_HTTP_HEADER, ProxiedEntitiesUtils.buildProxiedEntitiesChainString(user));
            }

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

    private void createBatchedActionLogStatement(StringBuilder strb, Collection<Action> actions) {
        strb.append("Cluster context too big for response header.  Replacing below actions with 'batch' action...\n");
        for (Action action : actions) {
            strb.append(ReflectionToStringBuilder.toString(action, ToStringStyle.MULTI_LINE_STYLE)).append("\n");
        }
    }
}
