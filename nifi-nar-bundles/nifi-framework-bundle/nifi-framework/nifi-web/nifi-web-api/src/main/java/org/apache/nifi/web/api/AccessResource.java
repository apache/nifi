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

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import io.jsonwebtoken.JwtException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.exception.AuthenticationNotSupportedException;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.idp.IdpType;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.AccessConfigurationDTO;
import org.apache.nifi.web.api.dto.AccessStatusDTO;
import org.apache.nifi.web.api.entity.AccessConfigurationEntity;
import org.apache.nifi.web.api.entity.AccessStatusEntity;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.apache.nifi.web.security.jwt.JwtAuthenticationProvider;
import org.apache.nifi.web.security.jwt.JwtAuthenticationRequestToken;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.kerberos.KerberosService;
import org.apache.nifi.web.security.knox.KnoxService;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.oidc.OidcService;
import org.apache.nifi.web.security.otp.OtpService;
import org.apache.nifi.web.security.saml.SAMLCredentialStore;
import org.apache.nifi.web.security.saml.SAMLEndpoints;
import org.apache.nifi.web.security.saml.SAMLService;
import org.apache.nifi.web.security.saml.SAMLStateManager;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.apache.nifi.web.security.token.OtpAuthenticationToken;
import org.apache.nifi.web.security.x509.X509AuthenticationProvider;
import org.apache.nifi.web.security.x509.X509AuthenticationRequestToken;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

import javax.servlet.ServletContext;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for managing access.
 */
@Path("/access")
@Api(
        value = "/access",
        description = "Endpoints for obtaining an access token or checking access status."
)
public class AccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(AccessResource.class);

    private static final String OIDC_REQUEST_IDENTIFIER = "oidc-request-identifier";
    private static final String OIDC_ID_TOKEN_AUTHN_ERROR = "Unable to exchange authorization for ID token: ";
    private static final String OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG = "OpenId Connect support is not configured";
    private static final String REVOKE_ACCESS_TOKEN_LOGOUT = "oidc_access_token_logout";
    private static final String ID_TOKEN_LOGOUT = "oidc_id_token_logout";
    private static final String STANDARD_LOGOUT = "oidc_standard_logout";
    private static final Pattern REVOKE_ACCESS_TOKEN_LOGOUT_FORMAT = Pattern.compile("(\\.google\\.com)");
    private static final Pattern ID_TOKEN_LOGOUT_FORMAT = Pattern.compile("(\\.okta)");
    private static final int msTimeout = 30_000;

    private static final String SAML_REQUEST_IDENTIFIER = "saml-request-identifier";
    private static final String SAML_METADATA_MEDIA_TYPE = "application/samlmetadata+xml";

    private static final String LOGIN_ERROR_TITLE = "Unable to continue login sequence";
    private static final String LOGOUT_ERROR_TITLE = "Unable to continue logout sequence";
    private static final String LOGOUT_REQUEST_IDENTIFIER = "nifi-logout-request-identifier";


    private static final String AUTHENTICATION_NOT_ENABLED_MSG = "User authentication/authorization is only supported when running over HTTPS.";
    private static final String LOGOUT_REQUEST_IDENTIFIER_NOT_FOUND = "The logout request identifier was not found in the request. Unable to continue.";
    private static final String LOGOUT_REQUEST_NOT_FOUND_FOR_GIVEN_IDENTIFIER = "No logout request was found for the given identifier. Unable to continue.";

    private X509CertificateExtractor certificateExtractor;
    private X509AuthenticationProvider x509AuthenticationProvider;
    private X509PrincipalExtractor principalExtractor;

    private LoginIdentityProvider loginIdentityProvider;
    private JwtAuthenticationProvider jwtAuthenticationProvider;
    private JwtService jwtService;
    private OtpService otpService;
    private OidcService oidcService;
    private KnoxService knoxService;
    private KerberosService kerberosService;

    private SAMLService samlService;
    private SAMLStateManager samlStateManager;
    private SAMLCredentialStore samlCredentialStore;

    private IdpUserGroupService idpUserGroupService;
    private LogoutRequestManager logoutRequestManager;

    /**
     * Retrieves the access configuration for this NiFi.
     *
     * @param httpServletRequest the servlet request
     * @return A accessConfigurationEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    @ApiOperation(
            value = "Retrieves the access configuration for this NiFi",
            response = AccessConfigurationEntity.class
    )
    public Response getLoginConfig(@Context HttpServletRequest httpServletRequest) {

        final AccessConfigurationDTO accessConfiguration = new AccessConfigurationDTO();

        // specify whether login should be supported and only support for secure requests
        accessConfiguration.setSupportsLogin(loginIdentityProvider != null && httpServletRequest.isSecure());

        // create the response entity
        final AccessConfigurationEntity entity = new AccessConfigurationEntity();
        entity.setConfig(accessConfiguration);

        // generate the response
        return generateOkResponse(entity).build();
    }

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

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

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

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

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

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

        // process the response from the idp...
        final Map<String, String> parameters = getParameterMap(uriInfo.getQueryParameters());
        samlLoginConsumer(httpServletRequest, httpServletResponse, parameters);
    }

    private void samlLoginConsumer(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Map<String, String> parameters) throws Exception {
        // ensure saml service provider is initialized
        initializeSamlServiceProvider();

        // ensure the request has the cookie with the request id
        final String samlRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), SAML_REQUEST_IDENTIFIER);
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
        final String samlRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), SAML_REQUEST_IDENTIFIER);
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

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

        // ensure the logout request identifier is present
        final String logoutRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), LOGOUT_REQUEST_IDENTIFIER);
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

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

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

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

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
        final String logoutRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), LOGOUT_REQUEST_IDENTIFIER);
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

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure saml is enabled
        if (!samlService.isSamlEnabled()) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, SAMLService.SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

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

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("oidc/request")
    @ApiOperation(
            value = "Initiates a request to authenticate through the configured OpenId Connect provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
            return;
        }

        // generate the authorization uri
        URI authorizationURI = oidcRequestAuthorizationCode(httpServletResponse, getOidcCallback());

        // generate the response
        httpServletResponse.sendRedirect(authorizationURI.toString());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("oidc/callback")
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the OpenId Connect login sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
            return;
        }

        final String oidcRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), OIDC_REQUEST_IDENTIFIER);
        if (oidcRequestIdentifier == null) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "The login request identifier was " +
                    "not found in the request. Unable to continue.");
            return;
        }

        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse;
        try {
            oidcResponse = AuthenticationResponseParser.parse(getRequestUri());
        } catch (final ParseException e) {
            logger.error("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue login process.");

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // forward to the error page
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Unable to parse the redirect URI " +
                    "from the OpenId Connect Provider. Unable to continue login process.");
            return;
        }

        if (oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            // confirm state
            final State state = successfulOidcResponse.getState();
            if (state == null || !oidcService.isStateValid(oidcRequestIdentifier, state)) {
                logger.error("The state value returned by the OpenId Connect Provider does not match the stored state. " +
                        "Unable to continue login process.");

                // remove the oidc request cookie
                removeOidcRequestCookie(httpServletResponse);

                // forward to the error page
                forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Purposed state does not match " +
                        "the stored state. Unable to continue login process.");
                return;
            }

            try {
                // exchange authorization code for id token
                final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
                final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcCallback()));

                // get the oidc token
                LoginAuthenticationToken oidcToken = oidcService.exchangeAuthorizationCodeForLoginAuthenticationToken(authorizationGrant);

                // exchange the oidc token for the NiFi token
                String nifiJwt = jwtService.generateSignedToken(oidcToken);

                // store the NiFi token
                oidcService.storeJwt(oidcRequestIdentifier, nifiJwt);

            } catch (final Exception e) {
                logger.error(OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage(), e);

                // remove the oidc request cookie
                removeOidcRequestCookie(httpServletResponse);

                // forward to the error page
                forwardToLoginMessagePage(httpServletRequest, httpServletResponse, OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage());
                return;
            }

            // redirect to the name page
            httpServletResponse.sendRedirect(getNiFiUri());
        } else {
            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful login
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Unsuccessful login attempt: "
                    + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("oidc/exchange")
    @ApiOperation(
            value = "Retrieves a JWT following a successful login sequence using the configured OpenId Connect provider.",
            response = String.class,
            notes = NON_GUARANTEED_ENDPOINT
    )
    public Response oidcExchange(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            logger.debug(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
            return Response.status(Response.Status.CONFLICT).entity(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG).build();
        }

        final String oidcRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), OIDC_REQUEST_IDENTIFIER);
        if (oidcRequestIdentifier == null) {
            final String message = "The login request identifier was not found in the request. Unable to continue.";
            logger.warn(message);
            return Response.status(Response.Status.BAD_REQUEST).entity(message).build();
        }

        // remove the oidc request cookie
        removeOidcRequestCookie(httpServletResponse);

        // get the jwt
        final String jwt = oidcService.getJwt(oidcRequestIdentifier);
        if (jwt == null) {
            throw new IllegalArgumentException("A JWT for this login request identifier could not be found. Unable to continue.");
        }

        // generate the response
        return generateOkResponse(jwt).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("oidc/logout")
    @ApiOperation(
            value = "Performs a logout in the OpenId Provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcLogout(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        if (!oidcService.isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
        }

        // Get the oidc discovery url
        String oidcDiscoveryUrl = properties.getOidcDiscoveryUrl();

        // Determine the logout method
        String logoutMethod = determineLogoutMethod(oidcDiscoveryUrl);

        switch (logoutMethod) {
            case REVOKE_ACCESS_TOKEN_LOGOUT:
            case ID_TOKEN_LOGOUT:
                // Make a request to the IdP
                URI authorizationURI = oidcRequestAuthorizationCode(httpServletResponse, getOidcLogoutCallback());
                httpServletResponse.sendRedirect(authorizationURI.toString());
                break;
            case STANDARD_LOGOUT:
            default:
                // Get the OIDC end session endpoint
                URI endSessionEndpoint = oidcService.getEndSessionEndpoint();
                String postLogoutRedirectUri = generateResourceUri( "..", "nifi", "logout-complete");

                if (endSessionEndpoint == null) {
                    httpServletResponse.sendRedirect(postLogoutRedirectUri);
                } else {
                    URI logoutUri = UriBuilder.fromUri(endSessionEndpoint)
                            .queryParam("post_logout_redirect_uri", postLogoutRedirectUri)
                            .build();
                    httpServletResponse.sendRedirect(logoutUri.toString());
                }
                break;
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("oidc/logoutCallback")
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the OpenId Connect logout sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcLogoutCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
            return;
        }

        final String oidcRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), OIDC_REQUEST_IDENTIFIER);
        if (oidcRequestIdentifier == null) {
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, "The login request identifier was " +
                    "not found in the request. Unable to continue.");
            return;
        }

        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse;
        try {
            oidcResponse = AuthenticationResponseParser.parse(getRequestUri());
        } catch (final ParseException e) {
            logger.error("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue " +
                    "logout process: " + e.getMessage(), e);

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // forward to the error page
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, "Unable to parse the redirect URI " +
                    "from the OpenId Connect Provider. Unable to continue logout process.");
            return;
        }

        if (oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            // confirm state
            final State state = successfulOidcResponse.getState();
            if (state == null || !oidcService.isStateValid(oidcRequestIdentifier, state)) {
                logger.error("The state value returned by the OpenId Connect Provider does not match the stored " +
                        "state. Unable to continue login process.");

                // remove the oidc request cookie
                removeOidcRequestCookie(httpServletResponse);

                // forward to the error page
                forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, "Purposed state does not match " +
                        "the stored state. Unable to continue login process.");
                return;
            }

            // Get the oidc discovery url
            String oidcDiscoveryUrl = properties.getOidcDiscoveryUrl();

            // Determine which logout method to use
            String logoutMethod = determineLogoutMethod(oidcDiscoveryUrl);

            // Get the authorization code and grant
            final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
            final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcLogoutCallback()));

            switch (logoutMethod) {
                case REVOKE_ACCESS_TOKEN_LOGOUT:
                    // Use the Revocation endpoint + access token
                    final String accessToken;
                    try {
                        // Return the access token
                        accessToken = oidcService.exchangeAuthorizationCodeForAccessToken(authorizationGrant);
                    } catch (final Exception e) {
                        logger.error("Unable to exchange authorization for the Access token: " + e.getMessage(), e);

                        // Remove the oidc request cookie
                        removeOidcRequestCookie(httpServletResponse);

                        // Forward to the error page
                        forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage());
                        return;
                    }

                    // Build the revoke URI and send the POST request
                    URI revokeEndpoint = getRevokeEndpoint();

                    if (revokeEndpoint != null) {
                        try {
                            // Logout with the revoke endpoint
                            revokeEndpointRequest(httpServletResponse, accessToken, revokeEndpoint);

                        } catch (final IOException e) {
                            logger.error("There was an error logging out of the OpenId Connect Provider: "
                                    + e.getMessage(), e);

                            // Remove the oidc request cookie
                            removeOidcRequestCookie(httpServletResponse);

                            // Forward to the error page
                            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse,
                                    "There was an error logging out of the OpenId Connect Provider: "
                                            + e.getMessage());
                        }
                    }
                    break;
                case ID_TOKEN_LOGOUT:
                    // Use the end session endpoint + ID Token
                    final String idToken;
                    try {
                        // Return the ID Token
                        idToken = oidcService.exchangeAuthorizationCodeForIdToken(authorizationGrant);
                    } catch (final Exception e) {
                        logger.error(OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage(), e);

                        // Remove the oidc request cookie
                        removeOidcRequestCookie(httpServletResponse);

                        // Forward to the error page
                        forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage());
                        return;
                    }

                    // Get the OIDC end session endpoint
                    URI endSessionEndpoint = oidcService.getEndSessionEndpoint();
                    String postLogoutRedirectUri = generateResourceUri("..", "nifi", "logout-complete");

                    if (endSessionEndpoint == null) {
                        logger.debug("Unable to log out of the OpenId Connect Provider. The end session endpoint is: null." +
                                " Redirecting to the logout page.");
                        httpServletResponse.sendRedirect(postLogoutRedirectUri);
                    } else {
                        URI logoutUri = UriBuilder.fromUri(endSessionEndpoint)
                                .queryParam("id_token_hint", idToken)
                                .queryParam("post_logout_redirect_uri", postLogoutRedirectUri)
                                .build();
                        httpServletResponse.sendRedirect(logoutUri.toString());
                    }
                    break;
            }
        } else {
            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful logout
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, "Unsuccessful logout attempt: "
                    + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("knox/request")
    @ApiOperation(
            value = "Initiates a request to authenticate through Apache Knox.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void knoxRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Apache Knox SSO support is not configured.");
            return;
        }

        // build the originalUri, and direct back to the ui
        final String originalUri = generateResourceUri("access", "knox", "callback");

        // build the authorization uri
        final URI authorizationUri = UriBuilder.fromUri(knoxService.getKnoxUrl())
                .queryParam("originalUrl", originalUri)
                .build();

        // generate the response
        httpServletResponse.sendRedirect(authorizationUri.toString());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("knox/callback")
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the Apache Knox login sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void knoxCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Apache Knox SSO support is not configured.");
            return;
        }

        httpServletResponse.sendRedirect(getNiFiUri());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("knox/logout")
    @ApiOperation(
            value = "Performs a logout in the Apache Knox.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void knoxLogout(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        String redirectPath = generateResourceUri("..", "nifi", "login");
        httpServletResponse.sendRedirect(redirectPath);
    }

    /**
     * Gets the status the client's access.
     *
     * @param httpServletRequest the servlet request
     * @return A accessStatusEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("")
    @ApiOperation(
            value = "Gets the status the client's access",
            notes = NON_GUARANTEED_ENDPOINT,
            response = AccessStatusEntity.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Unable to determine access status because the client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Unable to determine access status because the client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to determine access status because NiFi is not in the appropriate state."),
                    @ApiResponse(code = 500, message = "Unable to determine access status because an unexpected error occurred.")
            }
    )
    public Response getAccessStatus(@Context HttpServletRequest httpServletRequest) {

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        final AccessStatusDTO accessStatus = new AccessStatusDTO();

        try {
            final X509Certificate[] certificates = certificateExtractor.extractClientCertificate(httpServletRequest);

            // if there is not certificate, consider a token
            if (certificates == null) {
                // look for an authorization token
                final String authorization = httpServletRequest.getHeader(JwtAuthenticationFilter.AUTHORIZATION);

                // if there is no authorization header, we don't know the user
                if (authorization == null) {
                    accessStatus.setStatus(AccessStatusDTO.Status.UNKNOWN.name());
                    accessStatus.setMessage("No credentials supplied, unknown user.");
                } else {
                    try {
                        // Extract the Base64 encoded token from the Authorization header
                        final String token = StringUtils.substringAfterLast(authorization, " ");

                        final JwtAuthenticationRequestToken jwtRequest = new JwtAuthenticationRequestToken(token, httpServletRequest.getRemoteAddr());
                        final NiFiAuthenticationToken authenticationResponse = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(jwtRequest);
                        final NiFiUser nifiUser = ((NiFiUserDetails) authenticationResponse.getDetails()).getNiFiUser();

                        // set the user identity
                        accessStatus.setIdentity(nifiUser.getIdentity());

                        // attempt authorize to /flow
                        accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                        accessStatus.setMessage("You are already logged in.");
                    } catch (JwtException e) {
                        throw new InvalidAuthenticationException(e.getMessage(), e);
                    }
                }
            } else {
                try {
                    final String proxiedEntitiesChain = httpServletRequest.getHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN);
                    final String proxiedEntityGroups = httpServletRequest.getHeader(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS);

                    final X509AuthenticationRequestToken x509Request = new X509AuthenticationRequestToken(
                            proxiedEntitiesChain, proxiedEntityGroups, principalExtractor, certificates, httpServletRequest.getRemoteAddr());

                    final NiFiAuthenticationToken authenticationResponse = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(x509Request);
                    final NiFiUser nifiUser = ((NiFiUserDetails) authenticationResponse.getDetails()).getNiFiUser();

                    // set the user identity
                    accessStatus.setIdentity(nifiUser.getIdentity());

                    // attempt authorize to /flow
                    accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                    accessStatus.setMessage("You are already logged in.");
                } catch (final IllegalArgumentException iae) {
                    throw new InvalidAuthenticationException(iae.getMessage(), iae);
                }
            }
        } catch (final UntrustedProxyException upe) {
            throw new AccessDeniedException(upe.getMessage(), upe);
        } catch (final AuthenticationServiceException ase) {
            throw new AdministrationException(ase.getMessage(), ase);
        }

        // create the entity
        final AccessStatusEntity entity = new AccessStatusEntity();
        entity.setAccessStatus(accessStatus);

        return generateOkResponse(entity).build();
    }

    /**
     * Creates a single use access token for downloading FlowFile content.
     *
     * @param httpServletRequest the servlet request
     * @return A token (string)
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/download-token")
    @ApiOperation(
            value = "Creates a single use access token for downloading FlowFile content.",
            notes = "The token returned is a base64 encoded string. It is valid for a single request up to five minutes from being issued. " +
                    "It is used as a query parameter name 'access_token'.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to create the download token because NiFi is not in the appropriate state. " +
                            "(i.e. may not have any tokens to grant or be configured to support username/password login)"),
                    @ApiResponse(code = 500, message = "Unable to create download token because an unexpected error occurred.")
            }
    )
    public Response createDownloadToken(@Context HttpServletRequest httpServletRequest) {
        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Download tokens are only issued over HTTPS.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new AccessDeniedException("No user authenticated in the request.");
        }

        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(user.getIdentity());

        // generate otp for response
        final String token = otpService.generateDownloadToken(authenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "download-token"));
        return generateCreatedResponse(uri, token).build();
    }

    /**
     * Creates a single use access token for accessing a NiFi UI extension.
     *
     * @param httpServletRequest the servlet request
     * @return A token (string)
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/ui-extension-token")
    @ApiOperation(
            value = "Creates a single use access token for accessing a NiFi UI extension.",
            notes = "The token returned is a base64 encoded string. It is valid for a single request up to five minutes from being issued. " +
                    "It is used as a query parameter name 'access_token'.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to create the download token because NiFi is not in the appropriate state. " +
                            "(i.e. may not have any tokens to grant or be configured to support username/password login)"),
                    @ApiResponse(code = 500, message = "Unable to create download token because an unexpected error occurred.")
            }
    )
    public Response createUiExtensionToken(@Context HttpServletRequest httpServletRequest) {
        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException("UI extension access tokens are only issued over HTTPS.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new AccessDeniedException("No user authenticated in the request.");
        }

        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(user.getIdentity());

        // generate otp for response
        final String token = otpService.generateUiExtensionToken(authenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "ui-extension-token"));
        return generateCreatedResponse(uri, token).build();
    }

    /**
     * Creates a token for accessing the REST API via Kerberos ticket exchange / SPNEGO negotiation.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/kerberos")
    @ApiOperation(
            value = "Creates a token for accessing the REST API via Kerberos ticket exchange / SPNEGO negotiation",
            notes = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "NiFi was unable to complete the request because it did not contain a valid Kerberos " +
                            "ticket in the Authorization header. Retry this request after initializing a ticket with kinit and " +
                            "ensuring your browser is configured to support SPNEGO."),
                    @ApiResponse(code = 409, message = "Unable to create access token because NiFi is not in the appropriate state. (i.e. may not be configured to support Kerberos login."),
                    @ApiResponse(code = 500, message = "Unable to create access token because an unexpected error occurred.")
            }
    )
    public Response createAccessTokenFromTicket(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException("Access tokens are only issued over HTTPS.");
        }

        // If Kerberos Service Principal and keytab location not configured, throws exception
        if (!properties.isKerberosSpnegoSupportEnabled() || kerberosService == null) {
            final String message = "Kerberos ticket login not supported by this NiFi.";
            logger.debug(message);
            return Response.status(Response.Status.CONFLICT).entity(message).build();
        }

        String authorizationHeaderValue = httpServletRequest.getHeader(KerberosService.AUTHORIZATION_HEADER_NAME);

        if (!kerberosService.isValidKerberosHeader(authorizationHeaderValue)) {
            final Response response = generateNotAuthorizedResponse().header(KerberosService.AUTHENTICATION_CHALLENGE_HEADER_NAME, KerberosService.AUTHORIZATION_NEGOTIATE).build();
            return response;
        } else {
            try {
                // attempt to authenticate
                Authentication authentication = kerberosService.validateKerberosTicket(httpServletRequest);

                if (authentication == null) {
                    throw new IllegalArgumentException("Request is not HTTPS or Kerberos ticket missing or malformed");
                }

                final String expirationFromProperties = properties.getKerberosAuthenticationExpiration();
                long expiration = FormatUtils.getTimeDuration(expirationFromProperties, TimeUnit.MILLISECONDS);
                final String rawIdentity = authentication.getName();
                String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));
                expiration = validateTokenExpiration(expiration, mappedIdentity);

                // create the authentication token
                final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity, expiration, "KerberosService");

                // generate JWT for response
                final String token = jwtService.generateSignedToken(loginAuthenticationToken);

                // build the response
                final URI uri = URI.create(generateResourceUri("access", "kerberos"));
                return generateCreatedResponse(uri, token).build();
            } catch (final AuthenticationException e) {
                throw new AccessDeniedException(e.getMessage(), e);
            }
        }
    }

    /**
     * Creates a token for accessing the REST API via username/password.
     *
     * @param httpServletRequest the servlet request
     * @param username           the username
     * @param password           the password
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @ApiOperation(
            value = "Creates a token for accessing the REST API via username/password",
            notes = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to create access token because NiFi is not in the appropriate state. (i.e. may not be configured to support username/password login."),
                    @ApiResponse(code = 500, message = "Unable to create access token because an unexpected error occurred.")
            }
    )
    public Response createAccessToken(
            @Context HttpServletRequest httpServletRequest,
            @FormParam("username") String username,
            @FormParam("password") String password) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException("Access tokens are only issued over HTTPS.");
        }

        // if not configuration for login, don't consider credentials
        if (loginIdentityProvider == null) {
            throw new IllegalStateException("Username/Password login not supported by this NiFi.");
        }

        final LoginAuthenticationToken loginAuthenticationToken;

        // ensure we have login credentials
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("The username and password must be specified.");
        }

        try {
            // attempt to authenticate
            final AuthenticationResponse authenticationResponse = loginIdentityProvider.authenticate(new LoginCredentials(username, password));
            final String rawIdentity = authenticationResponse.getIdentity();
            String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));
            long expiration = validateTokenExpiration(authenticationResponse.getExpiration(), mappedIdentity);

            // create the authentication token
            loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity, expiration, authenticationResponse.getIssuer());
        } catch (final InvalidLoginCredentialsException ilce) {
            throw new IllegalArgumentException("The supplied username and password are not valid.", ilce);
        } catch (final IdentityAccessException iae) {
            throw new AdministrationException(iae.getMessage(), iae);
        }

        // generate JWT for response
        final String token = jwtService.generateSignedToken(loginAuthenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout")
    @ApiOperation(
            value = "Performs a logout for other providers that have been issued a JWT.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = "User was logged out successfully."),
                    @ApiResponse(code = 401, message = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(code = 500, message = "Client failed to log out."),
            }
    )
    public Response logOut(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        final String mappedUserIdentity = NiFiUserUtils.getNiFiUserIdentity();
        if (StringUtils.isBlank(mappedUserIdentity)) {
            return Response.status(Response.Status.UNAUTHORIZED)
                    .entity("Authentication token provided was empty or not in the correct JWT format.").build();
        }

        try {
            logger.info("Logging out " + mappedUserIdentity);
            jwtService.logOutUsingAuthHeader(httpServletRequest.getHeader(JwtAuthenticationFilter.AUTHORIZATION));
            logger.info("Successfully invalidated JWT for " + mappedUserIdentity);

            // create a LogoutRequest and tell the LogoutRequestManager about it for later retrieval
            final LogoutRequest logoutRequest = new LogoutRequest(UUID.randomUUID().toString(), mappedUserIdentity);
            logoutRequestManager.start(logoutRequest);

            // generate a cookie to store the logout request identifier
            final Cookie cookie = new Cookie(LOGOUT_REQUEST_IDENTIFIER, logoutRequest.getRequestIdentifier());
            cookie.setPath("/");
            cookie.setHttpOnly(true);
            cookie.setMaxAge(60);
            cookie.setSecure(true);
            httpServletResponse.addCookie(cookie);

            return generateOkResponse().build();
        } catch (final JwtException e) {
            logger.error("Logout of user " + mappedUserIdentity + " failed due to: " + e.getMessage(), e);
            return Response.serverError().build();
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout/complete")
    @ApiOperation(
            value = "Completes the logout sequence by removing the cached Logout Request and Cookie if they existed and redirects to /nifi/login.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = "User was logged out successfully."),
                    @ApiResponse(code = 401, message = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(code = 500, message = "Client failed to log out."),
            }
    )
    public void logOutComplete(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // complete the logout request by removing the cookie and cached request, if they were present
        completeLogoutRequest(httpServletResponse);

        // redirect to logout landing page
        httpServletResponse.sendRedirect(getNiFiLogoutCompleteUri());
    }

    private LogoutRequest completeLogoutRequest(final HttpServletResponse httpServletResponse) {
        LogoutRequest logoutRequest = null;

        // check if a logout request identifier is present and if so complete the request
        final String logoutRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), LOGOUT_REQUEST_IDENTIFIER);
        if (logoutRequestIdentifier != null) {
            logoutRequest = logoutRequestManager.complete(logoutRequestIdentifier);
        }

        if (logoutRequest == null) {
            logger.warn("Logout request did not exist for identifier: " + logoutRequestIdentifier);
        } else {
            logger.info("Completed logout request for " + logoutRequest.getMappedUserIdentity());
        }

        // remove the cookie if it existed
        removeLogoutRequestCookie(httpServletResponse);

        return logoutRequest;
    }

    private long validateTokenExpiration(long proposedTokenExpiration, String identity) {
        final long maxExpiration = TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);
        final long minExpiration = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

        if (proposedTokenExpiration > maxExpiration) {
            logger.warn(String.format("Max token expiration exceeded. Setting expiration to %s from %s for %s", maxExpiration,
                    proposedTokenExpiration, identity));
            proposedTokenExpiration = maxExpiration;
        } else if (proposedTokenExpiration < minExpiration) {
            logger.warn(String.format("Min token expiration not met. Setting expiration to %s from %s for %s", minExpiration,
                    proposedTokenExpiration, identity));
            proposedTokenExpiration = minExpiration;
        }

        return proposedTokenExpiration;
    }

    /**
     * Gets the value of a cookie matching the specified name. If no cookie with that name exists, null is returned.
     *
     * @param cookies the cookies
     * @param name    the name of the cookie
     * @return the value of the corresponding cookie, or null if the cookie does not exist
     */
    private String getCookieValue(final Cookie[] cookies, final String name) {
        if (cookies != null) {
            for (final Cookie cookie : cookies) {
                if (name.equals(cookie.getName())) {
                    return cookie.getValue();
                }
            }
        }

        return null;
    }

    private String getOidcCallback() {
        return generateResourceUri("access", "oidc", "callback");
    }

    private String getOidcLogoutCallback() {
        return generateResourceUri("access", "oidc", "logoutCallback");
    }

    private URI getRevokeEndpoint() {
        return oidcService.getRevocationEndpoint();
    }

    private String getNiFiUri() {
        final String nifiApiUrl = generateResourceUri();
        final String baseUrl = StringUtils.substringBeforeLast(nifiApiUrl, "/nifi-api");
        return baseUrl + "/nifi";
    }

    private String getNiFiLogoutCompleteUri() {
        return getNiFiUri() + "/logout-complete";
    }

    private void removeOidcRequestCookie(final HttpServletResponse httpServletResponse) {
        removeCookie(httpServletResponse, OIDC_REQUEST_IDENTIFIER);
    }

    private void removeSamlRequestCookie(final HttpServletResponse httpServletResponse) {
        removeCookie(httpServletResponse, SAML_REQUEST_IDENTIFIER);
    }

    private void removeLogoutRequestCookie(final HttpServletResponse httpServletResponse) {
        removeCookie(httpServletResponse, LOGOUT_REQUEST_IDENTIFIER);
    }

    private void removeCookie(final HttpServletResponse httpServletResponse, final String cookieName) {
        final Cookie cookie = new Cookie(cookieName, null);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(0);
        cookie.setSecure(true);
        httpServletResponse.addCookie(cookie);
    }

    private void forwardToLoginMessagePage(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse, final String message) throws Exception {
        forwardToMessagePage(httpServletRequest, httpServletResponse, LOGIN_ERROR_TITLE, message);
    }

    private void forwardToLogoutMessagePage(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse, final String message) throws Exception {
        forwardToMessagePage(httpServletRequest, httpServletResponse, LOGOUT_ERROR_TITLE, message);
    }

    private void forwardToMessagePage(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse,
                                      final String title, final String message) throws Exception {
        httpServletRequest.setAttribute("title", title);
        httpServletRequest.setAttribute("messages", message);

        final ServletContext uiContext = httpServletRequest.getServletContext().getContext("/nifi");
        uiContext.getRequestDispatcher("/WEB-INF/pages/message-page.jsp").forward(httpServletRequest, httpServletResponse);
    }

    private String determineLogoutMethod(String oidcDiscoveryUrl) {
        Matcher accessTokenMatcher = REVOKE_ACCESS_TOKEN_LOGOUT_FORMAT.matcher(oidcDiscoveryUrl);
        Matcher idTokenMatcher = ID_TOKEN_LOGOUT_FORMAT.matcher(oidcDiscoveryUrl);

        if (accessTokenMatcher.find()) {
            return REVOKE_ACCESS_TOKEN_LOGOUT;
        } else if (idTokenMatcher.find()) {
            return ID_TOKEN_LOGOUT;
        } else {
            return STANDARD_LOGOUT;
        }
    }

    /**
     * Generates the request Authorization URI for the OpenID Connect Provider. Returns an authorization
     * URI using the provided callback URI.
     *
     * @param httpServletResponse the servlet response
     * @param callback the OIDC callback URI
     * @return the authorization URI
     */
    private URI oidcRequestAuthorizationCode(@Context HttpServletResponse httpServletResponse, String callback) {

        final String oidcRequestIdentifier = UUID.randomUUID().toString();

        // generate a cookie to associate this login sequence
        final Cookie cookie = new Cookie(OIDC_REQUEST_IDENTIFIER, oidcRequestIdentifier);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(60);
        cookie.setSecure(true);
        httpServletResponse.addCookie(cookie);

        // get the state for this request
        final State state = oidcService.createState(oidcRequestIdentifier);

        // build the authorization uri
        final URI authorizationUri = UriBuilder.fromUri(oidcService.getAuthorizationEndpoint())
                .queryParam("client_id", oidcService.getClientId())
                .queryParam("response_type", "code")
                .queryParam("scope", oidcService.getScope().toString())
                .queryParam("state", state.getValue())
                .queryParam("redirect_uri", callback)
                .build();

        // return Authorization URI
        return authorizationUri;
    }

    /**
     * Sends a POST request to the revoke endpoint to log out of the ID Provider.
     *
     * @param httpServletResponse the servlet response
     * @param accessToken the OpenID Connect Provider access token
     * @param revokeEndpoint the name of the cookie
     * @throws IOException exceptional case for communication error with the OpenId Connect Provider
     */

    private void revokeEndpointRequest(@Context HttpServletResponse httpServletResponse, String accessToken, URI revokeEndpoint) throws IOException {

        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(msTimeout)
                .setConnectionRequestTimeout(msTimeout)
                .setSocketTimeout(msTimeout)
                .build();

        CloseableHttpClient httpClient = HttpClientBuilder
                .create()
                .setDefaultRequestConfig(config)
                .build();
        HttpPost httpPost = new HttpPost(revokeEndpoint);

        List<NameValuePair> params = new ArrayList<>();
        // Append a query param with the access token
        params.add(new BasicNameValuePair("token", accessToken));
        httpPost.setEntity(new UrlEncodedFormEntity(params));

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            httpClient.close();

            if (response.getStatusLine().getStatusCode() == HTTPResponse.SC_OK) {
                // Redirect to logout page
                logger.debug("You are logged out of the OpenId Connect Provider.");
                String postLogoutRedirectUri = generateResourceUri("..", "nifi", "logout-complete");
                httpServletResponse.sendRedirect(postLogoutRedirectUri);
            } else {
                logger.error("There was an error logging out of the OpenId Connect Provider. " +
                        "Response status: " + response.getStatusLine().getStatusCode());
            }
        }
    }

    // setters

    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setJwtAuthenticationProvider(JwtAuthenticationProvider jwtAuthenticationProvider) {
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
    }

    public void setKerberosService(KerberosService kerberosService) {
        this.kerberosService = kerberosService;
    }

    public void setX509AuthenticationProvider(X509AuthenticationProvider x509AuthenticationProvider) {
        this.x509AuthenticationProvider = x509AuthenticationProvider;
    }

    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    public void setOtpService(OtpService otpService) {
        this.otpService = otpService;
    }

    public void setOidcService(OidcService oidcService) {
        this.oidcService = oidcService;
    }

    public void setKnoxService(KnoxService knoxService) {
        this.knoxService = knoxService;
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

    public void setLogoutRequestManager(LogoutRequestManager logoutRequestManager) {
        this.logoutRequestManager = logoutRequestManager;
    }

}
