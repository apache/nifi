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

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.nifi.util.NiFiProperties;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.web.api.dto.AccessStatusDTO;
import org.apache.nifi.web.api.dto.AccessConfigurationDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AccessStatusEntity;
import org.apache.nifi.web.api.entity.AccessConfigurationEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.nifi.web.security.x509.X509IdentityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AccountStatusException;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

/**
 * RESTful endpoint for managing a cluster.
 */
@Path("/access")
@Api(
        value = "/access",
        description = "Endpoints for obtaining an access token or checking access status"
)
public class AccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(AccessResource.class);

    private static final String AUTHORIZATION = "Authorization";

    private NiFiProperties properties;

    private LoginIdentityProvider loginIdentityProvider;
    private X509CertificateExtractor certificateExtractor;
    private X509IdentityProvider certificateIdentityProvider;
    private JwtService jwtService;

    private AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService;

    /**
     * Retrieves the access configuration for this NiFi.
     *
     * @param httpServletRequest the servlet request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A accessConfigurationEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/config")
    @ApiOperation(
            value = "Retrieves the access configuration for this NiFi",
            response = AccessConfigurationEntity.class
    )
    public Response getLoginConfig(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        final AccessConfigurationDTO accessConfiguration = new AccessConfigurationDTO();

        // specify whether login should be supported and only support for secure requests
        accessConfiguration.setSupportsLogin(loginIdentityProvider != null && httpServletRequest.isSecure());
        accessConfiguration.setSupportsAnonymous(!properties.getAnonymousAuthorities().isEmpty() || !httpServletRequest.isSecure());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final AccessConfigurationEntity entity = new AccessConfigurationEntity();
        entity.setRevision(revision);
        entity.setConfig(accessConfiguration);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Gets the status the client's access.
     *
     * @param httpServletRequest the servlet request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A accessStatusEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Path("")
    @ApiOperation(
            value = "Gets the status the client's access",
            response = AccessStatusEntity.class
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getAccessStatus(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        final AccessStatusDTO accessStatus = new AccessStatusDTO();

        try {
            final X509Certificate[] certificates = certificateExtractor.extractClientCertificate(httpServletRequest);

            // if there is not certificate, consider a token
            if (certificates == null) {
                // look for an authorization token
                final String authorization = httpServletRequest.getHeader(AUTHORIZATION);

                // if there is no authorization header, we don't know the user
                if (authorization == null) {
                    accessStatus.setStatus(AccessStatusDTO.Status.UNKNOWN.name());
                    accessStatus.setMessage("No credentials supplied, unknown user.");
                } else {
                    // TODO - use this token with the JWT service
                    final String token = StringUtils.substringAfterLast(authorization, " ");

                    // TODO - do not call this method of the jwt service
                    final String principal = jwtService.getAuthentication(httpServletRequest);

                    // TODO - catch jwt exception?
                    // ensure we have something we can work with (certificate or crendentials)
                    if (principal == null) {
                        throw new IllegalArgumentException("The specific token is not valid.");
                    } else {
                        // set the user identity
                        accessStatus.setIdentity(principal);
                        accessStatus.setUsername(CertificateUtils.extractUsername(principal));

                        // without a certificate, this is not a proxied request
                        final List<String> chain = Arrays.asList(principal);

                        // check authorization for this user
                        checkAuthorization(chain);

                        // no issues with authorization
                        accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                        accessStatus.setMessage("Account is active and authorized");
                    }
                }
            } else {
                final AuthenticationResponse authenticationResponse = certificateIdentityProvider.authenticate(certificates);

                // get the proxy chain and ensure its populated
                final List<String> proxyChain = ProxiedEntitiesUtils.buildProxiedEntitiesChain(httpServletRequest, authenticationResponse.getIdentity());
                if (proxyChain.isEmpty()) {
                    logger.error(String.format("Unable to parse the proxy chain %s from the incoming request.", authenticationResponse.getIdentity()));
                    throw new IllegalArgumentException("Unable to determine the user from the incoming request.");
                }

                // ensure the proxy chain is authorized
                checkAuthorization(proxyChain);

                // set the user identity
                accessStatus.setIdentity(proxyChain.get(0));
                accessStatus.setUsername(CertificateUtils.extractUsername(proxyChain.get(0)));

                // no issues with authorization
                accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                accessStatus.setMessage("Account is active and authorized");
            }
        } catch (final UsernameNotFoundException unfe) {
            accessStatus.setStatus(AccessStatusDTO.Status.UNREGISTERED.name());
            accessStatus.setMessage(String.format("Unregistered user %s", accessStatus.getIdentity()));
        } catch (final AccountStatusException ase) {
            accessStatus.setStatus(AccessStatusDTO.Status.NOT_ACTIVE.name());
            accessStatus.setMessage(ase.getMessage());
        } catch (final UntrustedProxyException upe) {
            throw new AccessDeniedException(upe.getMessage(), upe);
        } catch (final AuthenticationServiceException ase) {
            throw new AdministrationException(ase.getMessage(), ase);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the entity
        final AccessStatusEntity entity = new AccessStatusEntity();
        entity.setRevision(revision);
        entity.setAccessStatus(accessStatus);

        return generateOkResponse(entity).build();
    }

    /**
     * Checks the status of the proxy.
     *
     * @param proxyChain the proxy chain
     * @throws AuthenticationException if the proxy chain is not authorized
     */
    private void checkAuthorization(final List<String> proxyChain) throws AuthenticationException {
        userDetailsService.loadUserDetails(new NiFiAuthenticationRequestToken(proxyChain));
    }

    /**
     * Creates a token for accessing the REST API via username/password.
     *
     * @param httpServletRequest the servlet request
     * @param username the username
     * @param password the password
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @ApiOperation(
            value = "Creates a token for accessing the REST API via username/password",
            response = String.class
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response createAccessToken(
            @Context HttpServletRequest httpServletRequest,
            @FormParam("username") String username,
            @FormParam("password") String password) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS.");
        }

        // if not configuration for login, don't consider credentials
        if (loginIdentityProvider == null) {
            throw new IllegalStateException("Username/Password login not supported by this NiFi.");
        }

        final LoginAuthenticationToken loginAuthenticationToken;

        final X509Certificate[] certificates = certificateExtractor.extractClientCertificate(httpServletRequest);

        // if there is not certificate, consider login credentials
        if (certificates == null) {
            // ensure we have login credentials
            if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
                throw new IllegalArgumentException("The username and password must be specified.");
            }

            try {
                // attempt to authenticate
                final AuthenticationResponse authenticationResponse = loginIdentityProvider.authenticate(new LoginCredentials(username, password));
                final long maxExpiration = TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);
                final long minExpiration = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
                
                long expiration = authenticationResponse.getExpiration();
                if (expiration > maxExpiration) {
                    expiration = maxExpiration;
                    
                    logger.warn(String.format("Max token expiration exceeded. Setting expiration to %s from %s for %s", expiration, 
                            authenticationResponse.getExpiration(), authenticationResponse.getIdentity()));
                } else if (expiration < minExpiration) {
                    expiration = minExpiration;
                    
                    logger.warn(String.format("Min token expiration not met. Setting expiration to %s from %s for %s", expiration, 
                            authenticationResponse.getExpiration(), authenticationResponse.getIdentity()));
                }
                
                // create the authentication token
                loginAuthenticationToken = new LoginAuthenticationToken(authenticationResponse.getIdentity(), expiration);
            } catch (final InvalidLoginCredentialsException ilce) {
                throw new IllegalArgumentException("The supplied username and password are not valid.", ilce);
            } catch (final IdentityAccessException iae) {
                throw new AdministrationException(iae.getMessage(), iae);
            }
        } else {
            // consider a certificate
            final AuthenticationResponse authenticationResponse = certificateIdentityProvider.authenticate(certificates);

            // get the proxy chain and ensure its populated
            final List<String> proxyChain = ProxiedEntitiesUtils.buildProxiedEntitiesChain(httpServletRequest, authenticationResponse.getIdentity());
            if (proxyChain.isEmpty()) {
                logger.error(String.format("Unable to parse the proxy chain %s from the incoming request.", authenticationResponse.getIdentity()));
                throw new IllegalArgumentException("Unable to determine the user from the incoming request.");
            }

            // authorize the proxy if necessary
            authorizeProxyIfNecessary(proxyChain);

            // create the authentication token
            loginAuthenticationToken = new LoginAuthenticationToken(proxyChain.get(0), authenticationResponse.getExpiration());
        }

        // generate JWT for response
        final String token = jwtService.generateSignedToken(loginAuthenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();
    }

    /**
     * Ensures the proxyChain is authorized before allowing the user to be authenticated.
     *
     * @param proxyChain the proxy chain
     * @throws AuthenticationException if the proxy chain is not authorized
     */
    private void authorizeProxyIfNecessary(final List<String> proxyChain) throws AuthenticationException {
        if (proxyChain.size() > 1) {
            try {
                userDetailsService.loadUserDetails(new NiFiAuthenticationRequestToken(proxyChain));
            } catch (final UsernameNotFoundException unfe) {
                // if a username not found exception was thrown, the proxies were authorized and now
                // we can issue a new token to the end user which they will use to identify themselves
                // when they enter a new account request
            } catch (final AuthenticationServiceException ase) {
                // throw an administration exception which will return a 500
                throw new AdministrationException(ase.getMessage(), ase);
            } catch (final Exception e) {
                // any other issue we're going to treat as access denied exception which will return 403
                throw new AccessDeniedException(e.getMessage(), e);
            }
        }
    }

    // setters
    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    public void setCertificateIdentityProvider(X509IdentityProvider certificateIdentityProvider) {
        this.certificateIdentityProvider = certificateIdentityProvider;
    }

    public void setUserDetailsService(AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

}
