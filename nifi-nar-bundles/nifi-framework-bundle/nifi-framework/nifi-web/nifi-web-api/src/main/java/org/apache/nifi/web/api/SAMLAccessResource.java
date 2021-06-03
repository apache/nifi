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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.authentication.exception.AuthenticationNotSupportedException;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.idp.IdpType;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.saml.SAMLCredentialStore;
import org.apache.nifi.web.security.saml.SAMLEndpoints;
import org.apache.nifi.web.security.saml.SAMLService;
import org.apache.nifi.web.security.saml.SAMLStateManager;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.web.util.WebUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Path(SAMLEndpoints.SAML_ACCESS_ROOT)
@Api(
        value = SAMLEndpoints.SAML_ACCESS_ROOT,
        description = "Endpoints for authenticating, obtaining an access token or logging out of a configured SAML authentication provider."
)
public class SAMLAccessResource extends AccessResource {

    private static final Logger logger = LoggerFactory.getLogger(SAMLAccessResource.class);
    private static final String SAML_REQUEST_IDENTIFIER = "saml-request-identifier";
    private static final String SAML_METADATA_MEDIA_TYPE = "application/samlmetadata+xml";
    private static final String LOGOUT_REQUEST_IDENTIFIER_NOT_FOUND = "The logout request identifier was not found in the request. Unable to continue.";
    private static final String LOGOUT_REQUEST_NOT_FOUND_FOR_GIVEN_IDENTIFIER = "No logout request was found for the given identifier. Unable to continue.";
    private static final boolean LOGGING_IN = true;

    private SAMLService samlService;
    private SAMLStateManager samlStateManager;
    private SAMLCredentialStore samlCredentialStore;
    private IdpUserGroupService idpUserGroupService;

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(SAML_METADATA_MEDIA_TYPE)
    @Path(SAMLEndpoints.SERVICE_PROVIDER_METADATA_RELATIVE)
    @ApiOperation(
            value = "Retrieves the service provider metadata.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public Response samlMetadata(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            logger.debug(SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return Response.status(Response.Status.CONFLICT).entity(SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED).build();
        }

        // ensure saml service provider is initialized
        initializeSamlServiceProvider();

        final String metadataXml = samlService.getServiceProviderMetadata();
        return Response.ok(metadataXml, SAML_METADATA_MEDIA_TYPE).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(SAMLEndpoints.LOGIN_REQUEST_RELATIVE)
    @ApiOperation(
            value = "Initiates an SSO request to the configured SAML identity provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void samlLoginRequest(@Context HttpServletRequest httpServletRequest,
                                 @Context HttpServletResponse httpServletResponse) throws Exception {

        assert(isSamlEnabled(httpServletRequest, httpServletResponse, LOGGING_IN));

        // ensure saml service provider is initialized
        initializeSamlServiceProvider();

        final String samlRequestIdentifier = UUID.randomUUID().toString();

        // generate a cookie to associate this login sequence
        final Cookie cookie = new Cookie(SAML_REQUEST_IDENTIFIER, samlRequestIdentifier);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(60);
        cookie.setSecure(true);
        httpServletResponse.addCookie(cookie);

        // get the state for this request
        final String relayState = samlStateManager.createState(samlRequestIdentifier);

        // initiate the login request
        try {
            samlService.initiateLogin(httpServletRequest, httpServletResponse, relayState);
        } catch (Exception e) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, e.getMessage());
            return;
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.WILDCARD)
    @Path(SAMLEndpoints.LOGIN_CONSUMER_RELATIVE)
    @ApiOperation(
            value = "Processes the SSO response from the SAML identity provider for HTTP-POST binding.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void samlLoginHttpPostConsumer(@Context HttpServletRequest httpServletRequest,
                                          @Context HttpServletResponse httpServletResponse,
                                          MultivaluedMap<String, String> formParams) throws Exception {

        assert(isSamlEnabled(httpServletRequest, httpServletResponse, LOGGING_IN));

        // process the response from the idp...
        final Map<String, String> parameters = getParameterMap(formParams);
        samlLoginConsumer(httpServletRequest, httpServletResponse, parameters);
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(SAMLEndpoints.LOGIN_CONSUMER_RELATIVE)
    @ApiOperation(
            value = "Processes the SSO response from the SAML identity provider for HTTP-REDIRECT binding.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void samlLoginHttpRedirectConsumer(@Context HttpServletRequest httpServletRequest,
                                              @Context HttpServletResponse httpServletResponse,
                                              @Context UriInfo uriInfo) throws Exception {

        assert(isSamlEnabled(httpServletRequest, httpServletResponse, LOGGING_IN));

        // process the response from the idp...
        final Map<String, String> parameters = getParameterMap(uriInfo.getQueryParameters());
        samlLoginConsumer(httpServletRequest, httpServletResponse, parameters);
    }

    private void samlLoginConsumer(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Map<String, String> parameters) throws Exception {
        // ensure saml service provider is initialized
        initializeSamlServiceProvider();

        // ensure the request has the cookie with the request id
        final String samlRequestIdentifier = WebUtils.getCookie(httpServletRequest, SAML_REQUEST_IDENTIFIER).getValue();
        if (samlRequestIdentifier == null) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "The login request identifier was not found in the request. Unable to continue.");
            return;
        }

        // ensure a RelayState value was sent back
        final String requestState = parameters.get("RelayState");
        if (requestState == null) {
            removeSamlRequestCookie(httpServletResponse);
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "The RelayState parameter was not found in the request. Unable to continue.");
            return;
        }

        // ensure the RelayState value in the request matches the store state
        if (!samlStateManager.isStateValid(samlRequestIdentifier, requestState)) {
            logger.error("The RelayState value returned by the SAML IDP does not match the stored state. Unable to continue login process.");
            removeSamlRequestCookie(httpServletResponse);
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Purposed RelayState does not match the stored state. Unable to continue login process.");
            return;
        }

        // process the SAML response
        final SAMLCredential samlCredential;
        try {
            samlCredential = samlService.processLogin(httpServletRequest, httpServletResponse, parameters);
        } catch (Exception e) {
            removeSamlRequestCookie(httpServletResponse);
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, e.getMessage());
            return;
        }

        // create the login token
        final String rawIdentity = samlService.getUserIdentity(samlCredential);
        final String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));
        final long expiration = validateTokenExpiration(samlService.getAuthExpiration(), mappedIdentity);
        final String issuer = samlCredential.getRemoteEntityID();

        final LoginAuthenticationToken loginToken = new LoginAuthenticationToken(mappedIdentity, mappedIdentity, expiration, issuer);

        // create and cache a NiFi JWT that can be retrieved later from the exchange end-point
        samlStateManager.createJwt(samlRequestIdentifier, loginToken);

        // store the SAMLCredential for retrieval during logout
        samlCredentialStore.save(mappedIdentity, samlCredential);

        // get the user's groups from the assertions if the exist and store them for later retrieval
        final Set<String> userGroups = samlService.getUserGroups(samlCredential);
        if (logger.isDebugEnabled()) {
            logger.debug("SAML User '{}' belongs to the unmapped groups {}", mappedIdentity, StringUtils.join(userGroups));
        }

        final List<IdentityMapping> groupIdentityMappings = IdentityMappingUtil.getGroupMappings(properties);
        final Set<String> mappedGroups = userGroups.stream()
                .map(g -> IdentityMappingUtil.mapIdentity(g, groupIdentityMappings))
                .collect(Collectors.toSet());
        logger.info("SAML User '{}' belongs to the mapped groups {}", mappedIdentity, StringUtils.join(mappedGroups));

        idpUserGroupService.replaceUserGroups(mappedIdentity, IdpType.SAML, mappedGroups);

        // redirect to the name page
        httpServletResponse.sendRedirect(getNiFiUri());
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path(SAMLEndpoints.LOGIN_EXCHANGE_RELATIVE)
    @ApiOperation(
            value = "Retrieves a JWT following a successful login sequence using the configured SAML identity provider.",
            response = String.class,
            notes = NON_GUARANTEED_ENDPOINT
    )
    public Response samlLoginExchange(@Context HttpServletRequest httpServletRequest,
                                      @Context HttpServletResponse httpServletResponse) throws Exception {

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            logger.debug(SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return Response.status(Response.Status.CONFLICT).entity(SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED).build();
        }

        logger.info("Attempting to exchange SAML login request for a NiFi JWT...");

        // ensure saml service provider is initialized
        initializeSamlServiceProvider();

        // ensure the request has the cookie with the request identifier
        final String samlRequestIdentifier = WebUtils.getCookie(httpServletRequest, SAML_REQUEST_IDENTIFIER).getValue();
        if (samlRequestIdentifier == null) {
            final String message = "The login request identifier was not found in the request. Unable to continue.";
            logger.warn(message);
            return Response.status(Response.Status.BAD_REQUEST).entity(message).build();
        }

        // remove the saml request cookie
        removeSamlRequestCookie(httpServletResponse);

        // get the jwt
        final String jwt = samlStateManager.getJwt(samlRequestIdentifier);
        if (jwt == null) {
            throw new IllegalArgumentException("A JWT for this login request identifier could not be found. Unable to continue.");
        }

        // generate the response
        logger.info("SAML login exchange complete");
        return generateOkResponse(jwt).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(SAMLEndpoints.SINGLE_LOGOUT_REQUEST_RELATIVE)
    @ApiOperation(
            value = "Initiates a logout request using the SingleLogout service of the configured SAML identity provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void samlSingleLogoutRequest(@Context HttpServletRequest httpServletRequest,
                                        @Context HttpServletResponse httpServletResponse) throws Exception {

        assert(isSamlEnabled(httpServletRequest, httpServletResponse, !LOGGING_IN));

        // ensure the logout request identifier is present
        final String logoutRequestIdentifier = WebUtils.getCookie(httpServletRequest, LOGOUT_REQUEST_IDENTIFIER).getValue();
        if (StringUtils.isBlank(logoutRequestIdentifier)) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, LOGOUT_REQUEST_IDENTIFIER_NOT_FOUND);
            return;
        }

        // ensure there is a logout request in progress for the given identifier
        final LogoutRequest logoutRequest = logoutRequestManager.get(logoutRequestIdentifier);
        if (logoutRequest == null) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, LOGOUT_REQUEST_NOT_FOUND_FOR_GIVEN_IDENTIFIER);
            return;
        }

        // ensure saml service provider is initialized
        initializeSamlServiceProvider();

        final String userIdentity = logoutRequest.getMappedUserIdentity();
        logger.info("Attempting to performing SAML Single Logout for {}", userIdentity);

        // retrieve the credential that was stored during the login sequence
        final SAMLCredential samlCredential = samlCredentialStore.get(userIdentity);
        if (samlCredential == null) {
            throw new IllegalStateException("Unable to find a stored SAML credential for " + userIdentity);
        }

        // initiate the logout
        try {
            logger.info("Initiating SAML Single Logout with IDP...");
            samlService.initiateLogout(httpServletRequest, httpServletResponse, samlCredential);
        } catch (Exception e) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, e.getMessage());
            return;
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(SAMLEndpoints.SINGLE_LOGOUT_CONSUMER_RELATIVE)
    @ApiOperation(
            value = "Processes a SingleLogout message from the configured SAML identity provider using the HTTP-REDIRECT binding.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void samlSingleLogoutHttpRedirectConsumer(@Context HttpServletRequest httpServletRequest,
                                                     @Context HttpServletResponse httpServletResponse,
                                                     @Context UriInfo uriInfo) throws Exception {

        assert(isSamlEnabled(httpServletRequest, httpServletResponse, !LOGGING_IN));

        // process the SLO request
        final Map<String, String> parameters = getParameterMap(uriInfo.getQueryParameters());
        samlSingleLogoutConsumer(httpServletRequest, httpServletResponse, parameters);
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(SAMLEndpoints.SINGLE_LOGOUT_CONSUMER_RELATIVE)
    @ApiOperation(
            value = "Processes a SingleLogout message from the configured SAML identity provider using the HTTP-POST binding.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void samlSingleLogoutHttpPostConsumer(@Context HttpServletRequest httpServletRequest,
                                                 @Context HttpServletResponse httpServletResponse,
                                                 MultivaluedMap<String, String> formParams) throws Exception {

        assert(isSamlEnabled(httpServletRequest, httpServletResponse, !LOGGING_IN));

        // process the SLO request
        final Map<String, String> parameters = getParameterMap(formParams);
        samlSingleLogoutConsumer(httpServletRequest, httpServletResponse, parameters);
    }

    /**
     * Common logic for consuming SAML Single Logout messages from either HTTP-POST or HTTP-REDIRECT.
     *
     * @param httpServletRequest the request
     * @param httpServletResponse the response
     * @param parameters additional parameters
     * @throws Exception if an error occurs
     */
    private void samlSingleLogoutConsumer(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
                                          Map<String, String> parameters) throws Exception {

        // ensure saml service provider is initialized
        initializeSamlServiceProvider();

        // ensure the logout request identifier is present
        final String logoutRequestIdentifier = WebUtils.getCookie(httpServletRequest, LOGOUT_REQUEST_IDENTIFIER).getValue();
        if (StringUtils.isBlank(logoutRequestIdentifier)) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, LOGOUT_REQUEST_IDENTIFIER_NOT_FOUND);
            return;
        }

        // ensure there is a logout request in progress for the given identifier
        final LogoutRequest logoutRequest = logoutRequestManager.get(logoutRequestIdentifier);
        if (logoutRequest == null) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, LOGOUT_REQUEST_NOT_FOUND_FOR_GIVEN_IDENTIFIER);
            return;
        }

        // complete the logout request so it is no longer cached
        logoutRequestManager.complete(logoutRequestIdentifier);

        // remove the cookie with the logout request identifier
        removeLogoutRequestCookie(httpServletResponse);

        // get the user identity from the logout request
        final String identity = logoutRequest.getMappedUserIdentity();
        logger.info("Consuming SAML Single Logout for {}", identity);

        // remove the saved credential
        samlCredentialStore.delete(identity);

        // delete any stored groups
        idpUserGroupService.deleteUserGroups(identity);

        // process the Single Logout SAML message
        try {
            samlService.processLogout(httpServletRequest, httpServletResponse, parameters);
            logger.info("Completed SAML Single Logout for {}", identity);
        } catch (Exception e) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, e.getMessage());
            return;
        }

        // redirect to the logout landing page
        httpServletResponse.sendRedirect(getNiFiLogoutCompleteUri());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(SAMLEndpoints.LOCAL_LOGOUT_RELATIVE)
    @ApiOperation(
            value = "Local logout when SAML is enabled, does not communicate with the IDP.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void samlLocalLogout(@Context HttpServletRequest httpServletRequest,
                                @Context HttpServletResponse httpServletResponse) throws Exception {

        assert(isSamlEnabled(httpServletRequest, httpServletResponse, !LOGGING_IN));

        // complete the logout request if one exists
        final LogoutRequest completedLogoutRequest = completeLogoutRequest(httpServletResponse);

        // if a logout request was completed, then delete the stored SAMLCredential for that user
        if (completedLogoutRequest != null) {
            final String userIdentity = completedLogoutRequest.getMappedUserIdentity();

            logger.info("Removing cached SAML information for " + userIdentity);
            samlCredentialStore.delete(userIdentity);

            logger.info("Removing cached SAML Groups for " + userIdentity);
            idpUserGroupService.deleteUserGroups(userIdentity);
        }

        // redirect to logout landing page
        httpServletResponse.sendRedirect(getNiFiLogoutCompleteUri());
    }

    private void initializeSamlServiceProvider() throws MetadataProviderException {
        if (!samlService.isServiceProviderInitialized()) {
            final String samlMetadataUri = generateResourceUri("saml", "metadata");
            final String baseUri = samlMetadataUri.replace("/saml/metadata", "");
            samlService.initializeServiceProvider(baseUri);
        }
    }

    private Map<String,String> getParameterMap(final MultivaluedMap<String, String> formParams) {
        final Map<String,String> params = new HashMap<>();
        for (final String paramKey : formParams.keySet()) {
            params.put(paramKey, formParams.getFirst(paramKey));
        }
        return params;
    }

    private void removeSamlRequestCookie(final HttpServletResponse httpServletResponse) {
        removeCookie(httpServletResponse, SAML_REQUEST_IDENTIFIER);
    }

    private boolean isSamlEnabled(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse, boolean isLogin) throws Exception {
        final String pageTitle = getForwardPageTitle(isLogin);

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle, AUTHENTICATION_NOT_ENABLED_MSG);
            return false;
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return false;
        }
        return true;
    }

    private String getForwardPageTitle(boolean isLogin) {
        return isLogin ? ApplicationResource.LOGIN_ERROR_TITLE : ApplicationResource.LOGOUT_ERROR_TITLE;
    }

    public void setSamlService(SAMLService samlService) {
        this.samlService = samlService;
    }

    public void setSamlStateManager(SAMLStateManager samlStateManager) {
        this.samlStateManager = samlStateManager;
    }

    public void setSamlCredentialStore(SAMLCredentialStore samlCredentialStore) {
        this.samlCredentialStore = samlCredentialStore;
    }

    public void setIdpUserGroupService(IdpUserGroupService idpUserGroupService) {
        this.idpUserGroupService = idpUserGroupService;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    protected NiFiProperties getProperties() {
        return properties;
    }
}