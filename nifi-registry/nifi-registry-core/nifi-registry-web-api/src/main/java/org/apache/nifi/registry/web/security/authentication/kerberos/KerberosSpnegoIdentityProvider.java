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
package org.apache.nifi.registry.web.security.authentication.kerberos;

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationResponse;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProviderConfigurationContext;
import org.apache.nifi.registry.security.authentication.IdentityProviderUsage;
import org.apache.nifi.registry.security.authentication.exception.IdentityAccessException;
import org.apache.nifi.registry.security.authentication.exception.InvalidCredentialsException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.security.authentication.AuthenticationDetailsSource;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider;
import org.springframework.security.kerberos.authentication.KerberosServiceRequestToken;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;

import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class KerberosSpnegoIdentityProvider implements IdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(KerberosSpnegoIdentityProvider.class);

    private static final String issuer = KerberosSpnegoIdentityProvider.class.getSimpleName();

    private static final IdentityProviderUsage usage = new IdentityProviderUsage() {
        @Override
        public String getText() {
            return "The Kerberos user credentials must be passed in the HTTP Authorization header as specified by SPNEGO-based Kerberos. " +
                    "That is: 'Authorization: Negotiate <kerberosTicket>', " +
                    "where <kerberosTicket> is a value that will be validated by this identity provider against a Kerberos cluster.";
        }

        @Override
        public AuthType getAuthType() {
            return AuthType.NEGOTIATE;
        }
    };

    private static final String AUTHORIZATION = "Authorization";
    private static final String AUTHORIZATION_NEGOTIATE = "Negotiate";

    private static final Base64.Decoder decoder = Base64.getDecoder();

    private long expiration = TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);
    private final KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider;
    private final AuthenticationDetailsSource<HttpServletRequest, ?> authenticationDetailsSource;

    @Autowired
    public KerberosSpnegoIdentityProvider(
            @Nullable  KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider,
            NiFiRegistryProperties properties) {
        this.kerberosServiceAuthenticationProvider = kerberosServiceAuthenticationProvider;
        authenticationDetailsSource = new WebAuthenticationDetailsSource();

        final String expirationFromProperties = properties.getKerberosSpnegoAuthenticationExpiration();
        if (expirationFromProperties != null) {
            expiration = Math.round(FormatUtils.getPreciseTimeDuration(expirationFromProperties, TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public IdentityProviderUsage getUsageInstructions() {
        return usage;
    }

    @Override
    public AuthenticationRequest extractCredentials(HttpServletRequest request) {

        // Only support Kerberos authentication when running securely
        if (!request.isSecure()) {
            return null;
        }

        String headerValue = request.getHeader(AUTHORIZATION);

        if (!isValidKerberosHeader(headerValue)) {
            return null;
        }

        logger.debug("Detected 'Authorization: Negotiate header in request {}", request.getRequestURL());
        byte[] base64Token = headerValue.substring(headerValue.indexOf(" ") + 1).getBytes(StandardCharsets.UTF_8);
        byte[] kerberosTicket = decoder.decode(base64Token);
        return new AuthenticationRequest(null, kerberosTicket, authenticationDetailsSource.buildDetails(request));

    }

    @Override
    public AuthenticationResponse authenticate(AuthenticationRequest authenticationRequest) throws InvalidCredentialsException, IdentityAccessException {

        if (authenticationRequest == null) {
            logger.info("Cannot authenticate null authenticationRequest, returning null.");
            return null;
        }

        final Object credentials = authenticationRequest.getCredentials();
        byte[] kerberosTicket = credentials instanceof byte[] ? (byte[]) authenticationRequest.getCredentials() : null;

        if (credentials == null) {
            logger.info("Kerberos Ticket not found in authenticationRequest credentials, returning null.");
            return null;
        }

        if (kerberosServiceAuthenticationProvider == null) {
            throw new IdentityAccessException("The Kerberos authentication provider is not initialized.");
        }

        try {
            KerberosServiceRequestToken kerberosServiceRequestToken = new KerberosServiceRequestToken(kerberosTicket);
            kerberosServiceRequestToken.setDetails(authenticationRequest.getDetails());
            Authentication authentication = kerberosServiceAuthenticationProvider.authenticate(kerberosServiceRequestToken);
            if (authentication == null) {
                throw new InvalidCredentialsException("Kerberos credentials could not be authenticated.");
            }

            final String kerberosPrincipal = authentication.getName();

            return new AuthenticationResponse(kerberosPrincipal, kerberosPrincipal, expiration, issuer);

        } catch (AuthenticationException e) {
            String authFailedMessage = "Kerberos credentials could not be authenticated.";
            logger.info(authFailedMessage);
            throw new InvalidCredentialsException(authFailedMessage, e);
        }

    }

    @Override
    public void onConfigured(IdentityProviderConfigurationContext configurationContext) throws SecurityProviderCreationException {
        throw new SecurityProviderCreationException(KerberosSpnegoIdentityProvider.class.getSimpleName() +
                " does not currently support being loaded via IdentityProviderFactory");
    }

    @Override
    public void preDestruction() throws SecurityProviderDestructionException {
    }

    public boolean isValidKerberosHeader(String headerValue) {
        return headerValue != null && (headerValue.startsWith(AUTHORIZATION_NEGOTIATE + " ") || headerValue.startsWith("Kerberos "));
    }
}
