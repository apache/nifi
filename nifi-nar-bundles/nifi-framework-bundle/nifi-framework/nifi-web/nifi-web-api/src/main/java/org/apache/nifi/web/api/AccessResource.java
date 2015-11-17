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
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.util.StringUtils;
import static org.apache.nifi.web.api.ApplicationResource.CLIENT_ID;
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
import org.apache.nifi.web.security.x509.X509CertificateValidator;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AccountStatusException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * RESTful endpoint for managing a cluster.
 */
@Path("/access")
@Api(
        value = "/access",
        description = "Endpoints for obtaining an access token or checking access status"
)
public class AccessResource extends ApplicationResource {

    private NiFiProperties properties;

    private X509CertificateValidator certificateValidator;
    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;

    private LoginIdentityProvider loginIdentityProvider;
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
            // look for a certificate
            final X509Certificate certificate = certificateExtractor.extractClientCertificate(httpServletRequest);

            // if no certificate, just check the credentials
            if (certificate == null) {
                final String principal = jwtService.getAuthentication(httpServletRequest);

                // ensure we have something we can work with (certificate or crendentials)
                if (principal == null) {
                    accessStatus.setStatus(AccessStatusDTO.Status.UNKNOWN.name());
                    accessStatus.setMessage("No credentials supplied, unknown user.");
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
                    accessStatus.setStatus("Account is active and authorized");
                }
            } else {
                // we have a certificate so let's consider a proxy chain
                final String principal = principalExtractor.extractPrincipal(certificate).toString();

                try {
                    // validate the certificate
                    certificateValidator.validateClientCertificate(httpServletRequest, certificate);
                } catch (CertificateExpiredException cee) {
                    throw new IllegalArgumentException(String.format("Client certificate for (%s) is expired.", principal), cee);
                } catch (CertificateNotYetValidException cnyve) {
                    throw new IllegalArgumentException(String.format("Client certificate for (%s) is not yet valid.", principal), cnyve);
                } catch (final Exception e) {
                    throw new IllegalArgumentException(e.getMessage(), e);
                }

                // set the user identity
                accessStatus.setIdentity(principal);
                accessStatus.setUsername(CertificateUtils.extractUsername(principal));

                // ensure the proxy chain is authorized
                checkAuthorization(ProxiedEntitiesUtils.buildProxyChain(httpServletRequest, principal));

                // no issues with authorization
                accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                accessStatus.setStatus("Account is active and authorized");
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

        // if we don't have username/password, consider JWT or x509
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            // look for a certificate
            final X509Certificate certificate = certificateExtractor.extractClientCertificate(httpServletRequest);

            // if there is no certificate, look for an existing token
            if (certificate == null) {
                // if not configured for login, don't consider existing tokens
                if (loginIdentityProvider == null) {
                    throw new IllegalStateException("Login not supported.");
                }

                // look for the principal
                final String principal = jwtService.getAuthentication(httpServletRequest);
                if (principal == null) {
                    throw new AuthenticationCredentialsNotFoundException("Unable to issue token as issue token as no credentials were found in the request.");
                }

                // create the authentication token
                loginAuthenticationToken = new LoginAuthenticationToken(principal, loginIdentityProvider.getExpiration());
            } else {
                // extract the principal
                final String principal = principalExtractor.extractPrincipal(certificate).toString();

                try {
                    certificateValidator.validateClientCertificate(httpServletRequest, certificate);
                } catch (CertificateExpiredException cee) {
                    throw new IllegalArgumentException(String.format("Client certificate for (%s) is expired.", principal), cee);
                } catch (CertificateNotYetValidException cnyve) {
                    throw new IllegalArgumentException(String.format("Client certificate for (%s) is not yet valid.", principal), cnyve);
                } catch (final Exception e) {
                    throw new IllegalArgumentException(e.getMessage(), e);
                }

                // authorize the proxy if necessary
                authorizeProxyIfNecessary(ProxiedEntitiesUtils.buildProxyChain(httpServletRequest, principal));

                // create the authentication token
                loginAuthenticationToken = new LoginAuthenticationToken(principal, loginIdentityProvider.getExpiration());
            }
        } else {
            try {
                // attempt to authenticate
                final AuthenticationResponse authenticationResponse = loginIdentityProvider.authenticate(new LoginCredentials(username, password));

                // create the authentication token
                loginAuthenticationToken = new LoginAuthenticationToken(authenticationResponse.getUsername(), loginIdentityProvider.getExpiration());
            } catch (final InvalidLoginCredentialsException ilce) {
                throw new IllegalArgumentException("The supplied username and password are not valid.", ilce);
            } catch (final IdentityAccessException iae) {
                throw new AdministrationException(iae.getMessage(), iae);
            }
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
                // we can issue a new ID token to the end user
            } catch (final Exception e) {
                // any other issue we're going to treat as an authentication exception which will return 401
                throw new AdministrationException(e.getMessage(), e) {
                };
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

    public void setCertificateValidator(X509CertificateValidator certificateValidator) {
        this.certificateValidator = certificateValidator;
    }

    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

    public void setUserDetailsService(AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

}
