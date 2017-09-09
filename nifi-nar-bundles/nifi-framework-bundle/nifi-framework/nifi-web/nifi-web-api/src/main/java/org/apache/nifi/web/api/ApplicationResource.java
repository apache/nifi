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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.representation.Form;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.server.impl.model.method.dispatch.FormDispatchProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.SnippetAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.util.CacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;

/**
 * Base class for controllers.
 */
public abstract class ApplicationResource {

    public static final String VERSION = "version";
    public static final String CLIENT_ID = "clientId";

    public static final String PROXY_SCHEME_HTTP_HEADER = "X-ProxyScheme";
    public static final String PROXY_HOST_HTTP_HEADER = "X-ProxyHost";
    public static final String PROXY_PORT_HTTP_HEADER = "X-ProxyPort";
    public static final String PROXY_CONTEXT_PATH_HTTP_HEADER = "X-ProxyContextPath";

    public static final String FORWARDED_PROTO_HTTP_HEADER = "X-Forwarded-Proto";
    public static final String FORWARDED_HOST_HTTP_HEADER = "X-Forwarded-Server";
    public static final String FORWARDED_PORT_HTTP_HEADER = "X-Forwarded-Port";
    public static final String FORWARDED_CONTEXT_HTTP_HEADER = "X-Forwarded-Context";

    protected static final String NON_GUARANTEED_ENDPOINT = "Note: This endpoint is subject to change as NiFi and it's REST API evolve.";

    private static final Logger logger = LoggerFactory.getLogger(ApplicationResource.class);

    public static final String NODEWISE = "false";

    @Context
    private HttpServletRequest httpServletRequest;

    @Context
    private UriInfo uriInfo;

    @Context
    private HttpContext httpContext;

    protected NiFiProperties properties;
    private RequestReplicator requestReplicator;
    private ClusterCoordinator clusterCoordinator;
    private FlowController flowController;

    private static final int MAX_CACHE_SOFT_LIMIT = 500;
    private final Cache<CacheKey, Request<? extends Entity>> twoPhaseCommitCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();

    /**
     * Generate a resource uri based off of the specified parameters.
     *
     * @param path path
     * @return resource uri
     */
    protected String generateResourceUri(final String... path) {
        final UriBuilder uriBuilder = uriInfo.getBaseUriBuilder();
        uriBuilder.segment(path);
        URI uri = uriBuilder.build();
        try {

            // check for proxy settings
            final String scheme = getFirstHeaderValue(PROXY_SCHEME_HTTP_HEADER, FORWARDED_PROTO_HTTP_HEADER);
            final String host = getFirstHeaderValue(PROXY_HOST_HTTP_HEADER, FORWARDED_HOST_HTTP_HEADER);
            final String port = getFirstHeaderValue(PROXY_PORT_HTTP_HEADER, FORWARDED_PORT_HTTP_HEADER);
            String baseContextPath = getFirstHeaderValue(PROXY_CONTEXT_PATH_HTTP_HEADER, FORWARDED_CONTEXT_HTTP_HEADER);

            // if necessary, prepend the context path
            String resourcePath = uri.getPath();
            if (baseContextPath != null) {
                // normalize context path
                if (!baseContextPath.startsWith("/")) {
                    baseContextPath = "/" + baseContextPath;
                }

                if (baseContextPath.endsWith("/")) {
                    baseContextPath = StringUtils.substringBeforeLast(baseContextPath, "/");
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
                    } catch (final NumberFormatException nfe) {
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
    protected ResponseBuilder noCache(final ResponseBuilder response) {
        final CacheControl cacheControl = new CacheControl();
        cacheControl.setPrivate(true);
        cacheControl.setNoCache(true);
        cacheControl.setNoStore(true);
        return response.cacheControl(cacheControl);
    }

    protected String generateUuid() {
        final Optional<String> seed = getIdGenerationSeed();
        UUID uuid;
        if (seed.isPresent()) {
            try {
                UUID seedId = UUID.fromString(seed.get());
                uuid = new UUID(seedId.getMostSignificantBits(), seed.get().hashCode());
            } catch (Exception e) {
                logger.warn("Provided 'seed' does not represent UUID. Will not be able to extract most significant bits for ID generation.");
                uuid = UUID.nameUUIDFromBytes(seed.get().getBytes(StandardCharsets.UTF_8));
            }
        } else {
            uuid = ComponentIdGenerator.generateId();
        }

        return uuid.toString();
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
    protected ResponseBuilder generateOkResponse(final Object entity) {
        final ResponseBuilder response = Response.ok(entity);
        return noCache(response);
    }

    /**
     * Generates a 201 Created response with the specified content.
     *
     * @param uri    The URI
     * @param entity entity
     * @return The response to be built
     */
    protected ResponseBuilder generateCreatedResponse(final URI uri, final Object entity) {
        // generate the response builder
        return Response.created(uri).entity(entity);
    }

    /**
     * Generates a 401 Not Authorized response with no content.
     *
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

    protected URI getRequestUri() {
        return uriInfo.getRequestUri();
    }

    protected MultivaluedMap<String, String> getRequestParameters() {
        final MultivaluedMap<String, String> entity = new MultivaluedMapImpl();

        // get the form that jersey processed and use it if it exists (only exist for requests with a body and application form urlencoded
        final Form form = (Form) httpContext.getProperties().get(FormDispatchProvider.FORM_PROPERTY);
        if (form == null) {
            for (final Map.Entry<String, String[]> entry : httpServletRequest.getParameterMap().entrySet()) {
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

    protected Map<String, String> getHeaders() {
        return getHeaders(new HashMap<String, String>());
    }

    protected Map<String, String> getHeaders(final Map<String, String> overriddenHeaders) {

        final Map<String, String> result = new HashMap<>();
        final Map<String, String> overriddenHeadersIgnoreCaseMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (overriddenHeaders != null) {
            overriddenHeadersIgnoreCaseMap.putAll(overriddenHeaders);
        }

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

        // if the scheme is not set by the client, include the details from this request but don't override
        final String proxyScheme = getFirstHeaderValue(PROXY_SCHEME_HTTP_HEADER, FORWARDED_PROTO_HTTP_HEADER);
        if (proxyScheme == null) {
            result.put(PROXY_SCHEME_HTTP_HEADER, httpServletRequest.getScheme());
        }

        // if the host is not set by the client, include the details from this request but don't override
        final String proxyHost = getFirstHeaderValue(PROXY_HOST_HTTP_HEADER, FORWARDED_HOST_HTTP_HEADER);
        if (proxyHost == null) {
            result.put(PROXY_HOST_HTTP_HEADER, httpServletRequest.getServerName());
        }

        // if the port is not set by the client, include the details from this request but don't override
        final String proxyPort = getFirstHeaderValue(PROXY_PORT_HTTP_HEADER, FORWARDED_PORT_HTTP_HEADER);
        if (proxyPort == null) {
            result.put(PROXY_PORT_HTTP_HEADER, String.valueOf(httpServletRequest.getServerPort()));
        }

        return result;
    }

    /**
     * Returns the value for the first key discovered when inspecting the current request. Will
     * return null if there are no keys specified or if none of the specified keys are found.
     *
     * @param keys http header keys
     * @return the value for the first key found
     */
    private String getFirstHeaderValue(final String... keys) {
        if (keys == null) {
            return null;
        }

        for (final String key : keys) {
            final String value = httpServletRequest.getHeader(key);

            // if we found an entry for this key, return the value
            if (value != null) {
                return value;
            }
        }

        // unable to find any matching keys
        return null;
    }

    /**
     * Checks whether the request is part of a two-phase commit style request (either phase 1 or phase 2)
     *
     * @param httpServletRequest the request
     * @return <code>true</code> if the request represents a two-phase commit style request
     */
    protected boolean isTwoPhaseRequest(final HttpServletRequest httpServletRequest) {
        final String transactionId = httpServletRequest.getHeader(RequestReplicator.REQUEST_TRANSACTION_ID_HEADER);
        return transactionId != null && isConnectedToCluster();
    }

    /**
     * When a two-phase commit style request is used, the first phase (generally referred to
     * as the "commit-request stage") is intended to validate that the request can be completed.
     * In NiFi, we use this phase to validate that the request can complete. This method determines
     * whether or not the request is the first phase of a two-phase commit.
     *
     * @param httpServletRequest the request
     * @return <code>true</code> if the request represents a two-phase commit style request and is the
     * first of the two phases.
     */
    protected boolean isValidationPhase(final HttpServletRequest httpServletRequest) {
        return isTwoPhaseRequest(httpServletRequest) && httpServletRequest.getHeader(RequestReplicator.REQUEST_VALIDATION_HTTP_HEADER) != null;
    }

    protected boolean isExecutionPhase(final HttpServletRequest httpServletRequest) {
        return isTwoPhaseRequest(httpServletRequest) && httpServletRequest.getHeader(RequestReplicator.REQUEST_EXECUTION_HTTP_HEADER) != null;
    }

    protected boolean isCancellationPhase(final HttpServletRequest httpServletRequest) {
        return isTwoPhaseRequest(httpServletRequest) && httpServletRequest.getHeader(RequestReplicator.REQUEST_TRANSACTION_CANCELATION_HTTP_HEADER) != null;
    }

    /**
     * Checks whether or not the request should be replicated to the cluster
     *
     * @return <code>true</code> if the request should be replicated, <code>false</code> otherwise
     */
    boolean isReplicateRequest() {
        // If not a node in a cluster, we do not replicate
        if (!properties.isNode()) {
            return false;
        }

        ensureFlowInitialized();

        // If not connected to the cluster, we do not replicate
        if (!isConnectedToCluster()) {
            return false;
        }

        // Check if the X-Request-Replicated header is set. If so, the request has already been replicated,
        // so we need to service the request locally. If not, then replicate the request to the entire cluster.
        final String header = httpServletRequest.getHeader(RequestReplicator.REPLICATION_INDICATOR_HEADER);
        return header == null;
    }

    /**
     * Converts a Revision DTO and an associated Component ID into a Revision object
     *
     * @param revisionDto the Revision DTO
     * @param componentId the ID of the component that the Revision DTO belongs to
     * @return a Revision that has the same client ID and Version as the Revision DTO and the Component ID specified
     */
    protected Revision getRevision(final RevisionDTO revisionDto, final String componentId) {
        return new Revision(revisionDto.getVersion(), revisionDto.getClientId(), componentId);
    }

    /**
     * Extracts a Revision object from the Revision DTO and ID provided by the Component Entity
     *
     * @param entity the ComponentEntity that contains the Revision DTO & ID
     * @return the Revision specified in the ComponentEntity
     */
    protected Revision getRevision(final ComponentEntity entity, final String componentId) {
        return getRevision(entity.getRevision(), componentId);
    }

    /**
     * Authorizes the specified process group.
     *
     * @param processGroupAuthorizable      process group
     * @param authorizer                    authorizer
     * @param lookup                        lookup
     * @param action                        action
     * @param authorizeReferencedServices   whether to authorize referenced services
     * @param authorizeTemplates            whether to authorize templates
     * @param authorizeControllerServices   whether to authorize controller services
     */
    protected void authorizeProcessGroup(final ProcessGroupAuthorizable processGroupAuthorizable, final Authorizer authorizer, final AuthorizableLookup lookup, final RequestAction action,
                                         final boolean authorizeReferencedServices, final boolean authorizeTemplates,
                                         final boolean authorizeControllerServices, final boolean authorizeTransitiveServices) {

        final Consumer<Authorizable> authorize = authorizable -> authorizable.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());

        // authorize the process group
        authorize.accept(processGroupAuthorizable.getAuthorizable());

        // authorize the contents of the group - these methods return all encapsulated components (recursive)
        processGroupAuthorizable.getEncapsulatedProcessors().forEach(processorAuthorizable -> {
            // authorize the processor
            authorize.accept(processorAuthorizable.getAuthorizable());

            // authorize any referenced services if necessary
            if (authorizeReferencedServices) {
                AuthorizeControllerServiceReference.authorizeControllerServiceReferences(processorAuthorizable, authorizer, lookup, authorizeTransitiveServices);
            }
        });
        processGroupAuthorizable.getEncapsulatedConnections().stream().map(connection -> connection.getAuthorizable()).forEach(authorize);
        processGroupAuthorizable.getEncapsulatedInputPorts().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedOutputPorts().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedFunnels().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedLabels().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedProcessGroups().stream().map(group -> group.getAuthorizable()).forEach(authorize);
        processGroupAuthorizable.getEncapsulatedRemoteProcessGroups().forEach(authorize);

        // authorize templates if necessary
        if (authorizeTemplates) {
            processGroupAuthorizable.getEncapsulatedTemplates().forEach(authorize);
        }

        // authorize controller services if necessary
        if (authorizeControllerServices) {
            processGroupAuthorizable.getEncapsulatedControllerServices().forEach(controllerServiceAuthorizable -> {
                // authorize the controller service
                authorize.accept(controllerServiceAuthorizable.getAuthorizable());

                // authorize any referenced services if necessary
                if (authorizeReferencedServices) {
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(controllerServiceAuthorizable, authorizer, lookup, authorizeTransitiveServices);
                }
            });
        }
    }

    /**
     * Authorizes the specified Snippet with the specified request action.
     *
     * @param authorizer authorizer
     * @param lookup     lookup
     * @param action     action
     */
    protected void authorizeSnippet(final SnippetAuthorizable snippet, final Authorizer authorizer, final AuthorizableLookup lookup, final RequestAction action,
                                    final boolean authorizeReferencedServices, final boolean authorizeTransitiveServices) {

        final Consumer<Authorizable> authorize = authorizable -> authorizable.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());

        // authorize each component in the specified snippet
        snippet.getSelectedProcessGroups().stream().forEach(processGroupAuthorizable -> {
            // note - we are not authorizing templates or controller services as they are not considered when using this snippet. however,
            // referenced services are considered so those are explicitly authorized when authorizing a processor
            authorizeProcessGroup(processGroupAuthorizable, authorizer, lookup, action, authorizeReferencedServices, false, false, authorizeTransitiveServices);
        });
        snippet.getSelectedRemoteProcessGroups().stream().forEach(authorize);
        snippet.getSelectedProcessors().stream().forEach(processorAuthorizable -> {
            // authorize the processor
            authorize.accept(processorAuthorizable.getAuthorizable());

            // authorize any referenced services if necessary
            if (authorizeReferencedServices) {
                AuthorizeControllerServiceReference.authorizeControllerServiceReferences(processorAuthorizable, authorizer, lookup, authorizeTransitiveServices);
            }
        });
        snippet.getSelectedInputPorts().stream().forEach(authorize);
        snippet.getSelectedOutputPorts().stream().forEach(authorize);
        snippet.getSelectedConnections().stream().forEach(connAuth -> authorize.accept(connAuth.getAuthorizable()));
        snippet.getSelectedFunnels().stream().forEach(authorize);
        snippet.getSelectedLabels().stream().forEach(authorize);
    }

    /**
     * Executes an action through the service facade using the specified revision.
     *
     * @param serviceFacade service facade
     * @param revision      revision
     * @param authorizer    authorizer
     * @param verifier      verifier
     * @param action        executor
     * @return the response
     */
    protected <T extends Entity> Response withWriteLock(final NiFiServiceFacade serviceFacade, final T entity, final Revision revision, final AuthorizeAccess authorizer,
                                                        final Runnable verifier, final BiFunction<Revision, T, Response> action) {

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (isTwoPhaseRequest(httpServletRequest)) {
            if (isValidationPhase(httpServletRequest)) {
                // authorize access
                serviceFacade.authorizeAccess(authorizer);
                serviceFacade.verifyRevision(revision, user);

                // verify if necessary
                if (verifier != null) {
                    verifier.run();
                }

                // store the request
                phaseOneStoreTransaction(entity, revision, null);

                return generateContinueResponse().build();
            } else if (isExecutionPhase(httpServletRequest)) {
                // get the original request and run the action
                final Request<T> phaseOneRequest = phaseTwoVerifyTransaction();
                return action.apply(phaseOneRequest.getRevision(), phaseOneRequest.getRequest());
            } else if (isCancellationPhase(httpServletRequest)) {
                cancelTransaction();
                return generateOkResponse().build();
            } else {
                throw new IllegalStateException("This request does not appear to be part of the two phase commit.");
            }
        } else {
            // authorize access and run the action
            serviceFacade.authorizeAccess(authorizer);
            serviceFacade.verifyRevision(revision, user);

            // verify if necessary
            if (verifier != null) {
                verifier.run();
            }

            return action.apply(revision, entity);
        }
    }

    /**
     * Executes an action through the service facade using the specified revision.
     *
     * @param serviceFacade service facade
     * @param revisions     revisions
     * @param authorizer    authorizer
     * @param verifier      verifier
     * @param action        executor
     * @return the response
     */
    protected <T extends Entity> Response withWriteLock(final NiFiServiceFacade serviceFacade, final T entity, final Set<Revision> revisions, final AuthorizeAccess authorizer,
                                                        final Runnable verifier, final BiFunction<Set<Revision>, T, Response> action) {

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (isTwoPhaseRequest(httpServletRequest)) {
            if (isValidationPhase(httpServletRequest)) {
                // authorize access
                serviceFacade.authorizeAccess(authorizer);
                serviceFacade.verifyRevisions(revisions, user);

                // verify if necessary
                if (verifier != null) {
                    verifier.run();
                }

                // store the request
                phaseOneStoreTransaction(entity, null, revisions);

                return generateContinueResponse().build();
            } else if (isExecutionPhase(httpServletRequest)) {
                // get the original request and run the action
                final Request<T> phaseOneRequest = phaseTwoVerifyTransaction();
                return action.apply(phaseOneRequest.getRevisions(), phaseOneRequest.getRequest());
            } else if (isCancellationPhase(httpServletRequest)) {
                cancelTransaction();
                return generateOkResponse().build();
            } else {
                throw new IllegalStateException("This request does not appear to be part of the two phase commit.");
            }
        } else {
            // authorize access and run the action
            serviceFacade.authorizeAccess(authorizer);
            serviceFacade.verifyRevisions(revisions, user);

            // verify if necessary
            if (verifier != null) {
                verifier.run();
            }

            return action.apply(revisions, entity);
        }
    }

    /**
     * Executes an action through the service facade.
     *
     * @param serviceFacade service facade
     * @param authorizer    authorizer
     * @param verifier      verifier
     * @param action        the action to execute
     * @return the response
     */
    protected <T extends Entity> Response withWriteLock(final NiFiServiceFacade serviceFacade, final T entity, final AuthorizeAccess authorizer,
                                                        final Runnable verifier, final Function<T, Response> action) {

        if (isTwoPhaseRequest(httpServletRequest)) {
            if (isValidationPhase(httpServletRequest)) {
                // authorize access
                serviceFacade.authorizeAccess(authorizer);

                // verify if necessary
                if (verifier != null) {
                    verifier.run();
                }

                // store the request
                phaseOneStoreTransaction(entity, null, null);

                return generateContinueResponse().build();
            } else if (isExecutionPhase(httpServletRequest)) {
                // get the original request and run the action
                final Request<T> phaseOneRequest = phaseTwoVerifyTransaction();
                return action.apply(phaseOneRequest.getRequest());
            } else if (isCancellationPhase(httpServletRequest)) {
                cancelTransaction();
                return generateOkResponse().build();
            } else {
                throw new IllegalStateException("This request does not appear to be part of the two phase commit.");
            }
        } else {
            // authorize access
            serviceFacade.authorizeAccess(authorizer);

            // verify if necessary
            if (verifier != null) {
                verifier.run();
            }

            // run the action
            return action.apply(entity);
        }
    }

    private <T extends Entity> void phaseOneStoreTransaction(final T requestEntity, final Revision revision, final Set<Revision> revisions) {
        if (twoPhaseCommitCache.size() > MAX_CACHE_SOFT_LIMIT) {
            throw new IllegalStateException("The maximum number of requests are in progress.");
        }

        // get the transaction id
        final String transactionId = httpServletRequest.getHeader(RequestReplicator.REQUEST_TRANSACTION_ID_HEADER);
        if (StringUtils.isBlank(transactionId)) {
            throw new IllegalArgumentException("Two phase commit Transaction Id missing.");
        }

        synchronized (twoPhaseCommitCache) {
            final CacheKey key = new CacheKey(transactionId);
            if (twoPhaseCommitCache.getIfPresent(key) != null) {
                throw new IllegalStateException("Transaction " + transactionId + " is already in progress.");
            }

            // store the entry for the second phase
            final NiFiUser user = NiFiUserUtils.getNiFiUser();
            final Request<T> request = new Request<>(ProxiedEntitiesUtils.buildProxiedEntitiesChainString(user), getAbsolutePath().toString(), revision, revisions, requestEntity);
            twoPhaseCommitCache.put(key, request);
        }
    }

    private <T extends Entity> Request<T> phaseTwoVerifyTransaction() {
        // get the transaction id
        final String transactionId = httpServletRequest.getHeader(RequestReplicator.REQUEST_TRANSACTION_ID_HEADER);
        if (StringUtils.isBlank(transactionId)) {
            throw new IllegalArgumentException("Two phase commit Transaction Id missing.");
        }

        // get the entry for the second phase
        final Request<T> request;
        synchronized (twoPhaseCommitCache) {
            final CacheKey key = new CacheKey(transactionId);
            request = (Request<T>) twoPhaseCommitCache.getIfPresent(key);
            if (request == null) {
                throw new IllegalArgumentException("The request from phase one is missing.");
            }

            twoPhaseCommitCache.invalidate(key);
        }
        final String phaseOneChain = request.getUserChain();

        // build the chain for the current request
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final String phaseTwoChain = ProxiedEntitiesUtils.buildProxiedEntitiesChainString(user);

        if (phaseOneChain == null || !phaseOneChain.equals(phaseTwoChain)) {
            throw new IllegalArgumentException("The same user must issue the request for phase one and two.");
        }

        final String phaseOneUri = request.getUri();
        if (phaseOneUri == null || !phaseOneUri.equals(getAbsolutePath().toString())) {
            throw new IllegalArgumentException("The URI must be the same for phase one and two.");
        }

        return request;
    }

    private void cancelTransaction() {
        // get the transaction id
        final String transactionId = httpServletRequest.getHeader(RequestReplicator.REQUEST_TRANSACTION_ID_HEADER);
        if (StringUtils.isBlank(transactionId)) {
            throw new IllegalArgumentException("Two phase commit Transaction Id missing.");
        }

        synchronized (twoPhaseCommitCache) {
            final CacheKey key = new CacheKey(transactionId);
            twoPhaseCommitCache.invalidate(key);
        }
    }

    private final class Request<T extends Entity> {
        final String userChain;
        final String uri;
        final Revision revision;
        final Set<Revision> revisions;
        final T request;

        public Request(String userChain, String uri, Revision revision, Set<Revision> revisions, T request) {
            this.userChain = userChain;
            this.uri = uri;
            this.revision = revision;
            this.revisions = revisions;
            this.request = request;
        }

        public String getUserChain() {
            return userChain;
        }

        public String getUri() {
            return uri;
        }

        public Revision getRevision() {
            return revision;
        }

        public Set<Revision> getRevisions() {
            return revisions;
        }

        public T getRequest() {
            return request;
        }
    }

    /**
     * Replicates the request to the given node
     *
     * @param method   the HTTP method
     * @param nodeUuid the UUID of the node to replicate the request to
     * @return the response from the node
     * @throws UnknownNodeException if the nodeUuid given does not map to any node in the cluster
     */
    protected Response replicate(final String method, final String nodeUuid) {
        return replicate(method, getRequestParameters(), nodeUuid);
    }

    private void ensureFlowInitialized() {
        if (!flowController.isInitialized()) {
            throw new IllegalClusterStateException("Cluster is still in the process of voting on the appropriate Data Flow.");
        }
    }

    protected Response replicate(final String method, final Object entity, final String nodeUuid, final Map<String, String> headersToOverride) {
        final URI path = getAbsolutePath();
        return replicate(path, method, entity, nodeUuid, headersToOverride);
    }

    /**
     * Replicates the request to the given node
     *
     * @param method   the HTTP method
     * @param entity   the Entity to replicate
     * @param nodeUuid the UUID of the node to replicate the request to
     * @return the response from the node
     * @throws UnknownNodeException if the nodeUuid given does not map to any node in the cluster
     */
    protected Response replicate(final String method, final Object entity, final String nodeUuid) {
        return replicate(method, entity, nodeUuid, null);
    }

    /**
     * Replicates the request to the given node
     *
     * @param method the HTTP method
     * @param entity the Entity to replicate
     * @param nodeUuid the UUID of the node to replicate the request to
     * @return the response from the node
     * @throws UnknownNodeException if the nodeUuid given does not map to any node in the cluster
     */
    protected Response replicate(final URI path, final String method, final Object entity, final String nodeUuid, final Map<String, String> headersToOverride) {
        // since we're cluster we must specify the cluster node identifier
        if (nodeUuid == null) {
            throw new IllegalArgumentException("The cluster node identifier must be specified.");
        }

        final NodeIdentifier nodeId = clusterCoordinator.getNodeIdentifier(nodeUuid);
        if (nodeId == null) {
            throw new UnknownNodeException("Cannot replicate request " + method + " " + getAbsolutePath() + " to node with ID " + nodeUuid + " because the specified node does not exist.");
        }

        ensureFlowInitialized();

        try {
            final Map<String, String> headers = headersToOverride == null ? getHeaders() : getHeaders(headersToOverride);

            // Determine if we should replicate to the node directly or if we should replicate to the Cluster Coordinator,
            // and have it replicate the request on our behalf.
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                // If we are to replicate directly to the nodes, we need to indicate that the replication source is
                // the cluster coordinator so that the node knows to service the request.
                final Set<NodeIdentifier> targetNodes = Collections.singleton(nodeId);
                return requestReplicator.replicate(targetNodes, method, path, entity, headers, true, true).awaitMergedResponse().getResponse();
            } else {
                headers.put(RequestReplicator.REPLICATION_TARGET_NODE_UUID_HEADER, nodeId.getId());
                return requestReplicator.forwardToCoordinator(getClusterCoordinatorNode(), method, path, entity, headers).awaitMergedResponse().getResponse();
            }
        } catch (final InterruptedException ie) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Request to " + method + " " + path + " was interrupted").type("text/plain").build();
        }
    }

    protected NodeIdentifier getClusterCoordinatorNode() {
        final NodeIdentifier activeClusterCoordinator = clusterCoordinator.getElectedActiveCoordinatorNode();
        if (activeClusterCoordinator != null) {
            return activeClusterCoordinator;
        }

        throw new NoClusterCoordinatorException();
    }

    protected ReplicationTarget getReplicationTarget() {
        return clusterCoordinator.isActiveClusterCoordinator() ? ReplicationTarget.CLUSTER_NODES : ReplicationTarget.CLUSTER_COORDINATOR;
    }


    protected Response replicate(final String method, final NodeIdentifier targetNode) {
        return replicate(method, targetNode, getRequestParameters());
    }

    protected Response replicate(final String method, final NodeIdentifier targetNode, final Object entity) {
        ensureFlowInitialized();

        try {
            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly
            // to the cluster nodes themselves.
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                final Set<NodeIdentifier> nodeIds = Collections.singleton(targetNode);
                return getRequestReplicator().replicate(nodeIds, method, getAbsolutePath(), entity, getHeaders(), true, true).awaitMergedResponse().getResponse();
            } else {
                final Map<String, String> headers = getHeaders(Collections.singletonMap(RequestReplicator.REPLICATION_TARGET_NODE_UUID_HEADER, targetNode.getId()));
                return requestReplicator.forwardToCoordinator(getClusterCoordinatorNode(), method, getAbsolutePath(), entity, headers).awaitMergedResponse().getResponse();
            }
        } catch (final InterruptedException ie) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Request to " + method + " " + getAbsolutePath() + " was interrupted").type("text/plain").build();
        }
    }

    protected Response replicateToCoordinator(final String method, final Object entity) {
        ensureFlowInitialized();

        try {
            final NodeIdentifier coordinatorNode = getClusterCoordinatorNode();
            final Set<NodeIdentifier> coordinatorNodes = Collections.singleton(coordinatorNode);
            return getRequestReplicator().replicate(coordinatorNodes, method, getAbsolutePath(), entity, getHeaders(), true, false).awaitMergedResponse().getResponse();
        } catch (final InterruptedException ie) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Request to " + method + " " + getAbsolutePath() + " was interrupted").type("text/plain").build();
        }
    }

    /**
     * Convenience method for calling {@link #replicate(String, Object)} with an entity of
     * {@link #getRequestParameters() getRequestParameters(true)}
     *
     * @param method the HTTP method to use
     * @return the response from the request
     */
    protected Response replicate(final String method) {
        return replicate(method, getRequestParameters());
    }

    /**
     * Convenience method for calling {@link #replicateNodeResponse(String, Object, Map)} with an entity of
     * {@link #getRequestParameters() getRequestParameters(true)} and overriding no headers
     *
     * @param method the HTTP method to use
     * @return the response from the request
     * @throws InterruptedException if interrupted while replicating the request
     */
    protected NodeResponse replicateNodeResponse(final String method) throws InterruptedException {
        return replicateNodeResponse(method, getRequestParameters(), (Map<String, String>) null);
    }

    /**
     * Replicates the request to all nodes in the cluster using the provided method and entity. The headers
     * used will be those provided by the {@link #getHeaders()} method. The URI that will be used will be
     * that provided by the {@link #getAbsolutePath()} method
     *
     * @param method the HTTP method to use
     * @param entity the entity to replicate
     * @return the response from the request
     */
    protected Response replicate(final String method, final Object entity) {
        return replicate(method, entity, (Map<String, String>) null);
    }

    /**
     * Replicates the request to all nodes in the cluster using the provided method and entity. The headers
     * used will be those provided by the {@link #getHeaders()} method. The URI that will be used will be
     * that provided by the {@link #getAbsolutePath()} method
     *
     * @param method            the HTTP method to use
     * @param entity            the entity to replicate
     * @param headersToOverride the headers to override
     * @return the response from the request
     * @see #replicateNodeResponse(String, Object, Map)
     */
    protected Response replicate(final String method, final Object entity, final Map<String, String> headersToOverride) {
        try {
            return replicateNodeResponse(method, entity, headersToOverride).getResponse();
        } catch (final InterruptedException ie) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Request to " + method + " " + getAbsolutePath() + " was interrupted").type("text/plain").build();
        }
    }


    protected NodeResponse replicateNodeResponse(final String method, final Object entity, final Map<String, String> headersToOverride) throws InterruptedException {
        final URI path = getAbsolutePath();
        return replicateNodeResponse(path, method, entity, headersToOverride);
    }

    /**
     * Replicates the request to all nodes in the cluster using the provided method and entity. The headers
     * used will be those provided by the {@link #getHeaders()} method. The URI that will be used will be
     * that provided by the {@link #getAbsolutePath()} method. This method returns the NodeResponse,
     * rather than a Response object.
     *
     * @param method            the HTTP method to use
     * @param entity            the entity to replicate
     * @param headersToOverride the headers to override
     * @return the response from the request
     * @throws InterruptedException if interrupted while replicating the request
     * @see #replicate(String, Object, Map)
     */
    protected NodeResponse replicateNodeResponse(final URI path, final String method, final Object entity, final Map<String, String> headersToOverride) throws InterruptedException {
        ensureFlowInitialized();

        final Map<String, String> headers = headersToOverride == null ? getHeaders() : getHeaders(headersToOverride);

        // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly
        // to the cluster nodes themselves.
        final long replicateStart = System.nanoTime();
        String action = null;
        try {
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                action = "Replicate Request " + method + " " + path;
                return requestReplicator.replicate(method, path, entity, headers).awaitMergedResponse();
            } else {
                action = "Forward Request " + method + " " + path + " to Coordinator";
                return requestReplicator.forwardToCoordinator(getClusterCoordinatorNode(), method, path, entity, headers).awaitMergedResponse();
            }
        } finally {
            final long replicateNanos = System.nanoTime() - replicateStart;
            final String transactionId = headers.get(RequestReplicator.REQUEST_TRANSACTION_ID_HEADER);
            final String requestId = transactionId == null ? "Request with no ID" : transactionId;
            logger.debug("Took a total of {} millis to {} for {}", TimeUnit.NANOSECONDS.toMillis(replicateNanos), action, requestId);
        }
    }

    /**
     * @return <code>true</code> if connected to a cluster, <code>false</code>
     * if running in standalone mode or disconnected from cluster
     */
    boolean isConnectedToCluster() {
        return isClustered() && clusterCoordinator.isConnected();
    }

    boolean isClustered() {
        return clusterCoordinator != null;
    }

    public void setRequestReplicator(final RequestReplicator requestReplicator) {
        this.requestReplicator = requestReplicator;
    }

    protected RequestReplicator getRequestReplicator() {
        ensureFlowInitialized();

        return requestReplicator;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    protected ClusterCoordinator getClusterCoordinator() {
        return clusterCoordinator;
    }

    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    protected NiFiProperties getProperties() {
        return properties;
    }

    public static enum ReplicationTarget {
        CLUSTER_NODES, CLUSTER_COORDINATOR;
    }

    // -----------------
    // HTTP site to site
    // -----------------

    protected Integer negotiateTransportProtocolVersion(final HttpServletRequest req, final VersionNegotiator transportProtocolVersionNegotiator) throws BadRequestException {
        String protocolVersionStr = req.getHeader(HttpHeaders.PROTOCOL_VERSION);
        if (isEmpty(protocolVersionStr)) {
            throw new BadRequestException("Protocol version was not specified.");
        }

        final Integer requestedProtocolVersion;
        try {
            requestedProtocolVersion = Integer.valueOf(protocolVersionStr);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Specified protocol version was not in a valid number format: " + protocolVersionStr);
        }

        Integer protocolVersion;
        if (transportProtocolVersionNegotiator.isVersionSupported(requestedProtocolVersion)) {
            return requestedProtocolVersion;
        } else {
            protocolVersion = transportProtocolVersionNegotiator.getPreferredVersion(requestedProtocolVersion);
        }

        if (protocolVersion == null) {
            throw new BadRequestException("Specified protocol version is not supported: " + protocolVersionStr);
        }
        return protocolVersion;
    }

    protected Response.ResponseBuilder setCommonHeaders(final Response.ResponseBuilder builder, final Integer transportProtocolVersion, final HttpRemoteSiteListener transactionManager) {
        return builder.header(HttpHeaders.PROTOCOL_VERSION, transportProtocolVersion)
                .header(HttpHeaders.SERVER_SIDE_TRANSACTION_TTL, transactionManager.getTransactionTtlSec());
    }

    protected class ResponseCreator {

        public Response nodeTypeErrorResponse(String errMsg) {
            return noCache(Response.status(Response.Status.FORBIDDEN)).type(MediaType.TEXT_PLAIN).entity(errMsg).build();
        }

        public Response httpSiteToSiteIsNotEnabledResponse() {
            return noCache(Response.status(Response.Status.FORBIDDEN)).type(MediaType.TEXT_PLAIN).entity("HTTP(S) Site-to-Site is not enabled on this host.").build();
        }

        public Response wrongPortTypeResponse(String portType, String portId) {
            logger.debug("Port type was wrong. portType={}, portId={}", portType, portId);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Port was not found.");
            entity.setFlowFileSent(0);
            return Response.status(NOT_FOUND).entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        public Response transactionNotFoundResponse(String portId, String transactionId) {
            logger.debug("Transaction was not found. portId={}, transactionId={}", portId, transactionId);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Transaction was not found.");
            entity.setFlowFileSent(0);
            return Response.status(NOT_FOUND).entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        public Response unexpectedErrorResponse(String portId, Exception e) {
            logger.error("Unexpected exception occurred. portId={}", portId);
            logger.error("Exception detail:", e);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Server encountered an exception.");
            entity.setFlowFileSent(0);
            return Response.serverError().entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        public Response unexpectedErrorResponse(String portId, String transactionId, Exception e) {
            logger.error("Unexpected exception occurred. portId={}, transactionId={}", portId, transactionId);
            logger.error("Exception detail:", e);
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage("Server encountered an exception.");
            entity.setFlowFileSent(0);
            return Response.serverError().entity(entity).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        public Response unauthorizedResponse(NotAuthorizedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Client request was not authorized. {}", e.getMessage());
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.UNAUTHORIZED.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            return Response.status(Response.Status.UNAUTHORIZED).type(MediaType.APPLICATION_JSON_TYPE).entity(e.getMessage()).build();
        }

        public Response badRequestResponse(Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Client sent a bad request. {}", e.getMessage());
            }
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.ABORT.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
        }

        public Response handshakeExceptionResponse(HandshakeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Handshake failed, {}", e.getMessage());
            }
            ResponseCode handshakeRes = e.getResponseCode();
            Response.Status statusCd;
            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(handshakeRes != null ? handshakeRes.getCode() : ResponseCode.ABORT.getCode());
            entity.setMessage(e.getMessage());
            entity.setFlowFileSent(0);
            switch (handshakeRes) {
                case PORT_NOT_IN_VALID_STATE:
                case PORTS_DESTINATION_FULL:
                    return Response.status(Response.Status.SERVICE_UNAVAILABLE).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
                case UNAUTHORIZED:
                    statusCd = Response.Status.UNAUTHORIZED;
                    break;
                case UNKNOWN_PORT:
                    statusCd = NOT_FOUND;
                    break;
                default:
                    statusCd = Response.Status.BAD_REQUEST;
            }
            return Response.status(statusCd).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
        }

        public Response acceptedResponse(final HttpRemoteSiteListener transactionManager, final Object entity, final Integer protocolVersion) {
            return noCache(setCommonHeaders(Response.status(Response.Status.ACCEPTED), protocolVersion, transactionManager))
                    .entity(entity).build();
        }

        public Response locationResponse(UriInfo uriInfo, String portType, String portId, String transactionId, Object entity,
                                         Integer protocolVersion, final HttpRemoteSiteListener transactionManager) {

            String path = "/data-transfer/" + portType + "/" + portId + "/transactions/" + transactionId;
            URI location = uriInfo.getBaseUriBuilder().path(path).build();
            return noCache(setCommonHeaders(Response.created(location), protocolVersion, transactionManager)
                    .header(LOCATION_URI_INTENT_NAME, LOCATION_URI_INTENT_VALUE))
                    .entity(entity).build();
        }

    }
}
