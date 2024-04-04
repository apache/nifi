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
package org.apache.nifi.web.security.jwt.converter;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.PlainJWT;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoderFactory;
import org.springframework.security.oauth2.jwt.JwtException;

import java.util.Objects;

/**
 * JSON Web Token Decoder capable of delegating to specific Decoder based on Issuer claims for OpenID Connect Clients
 */
public class StandardIssuerJwtDecoder implements JwtDecoder {
    private static final Logger logger = LoggerFactory.getLogger(StandardIssuerJwtDecoder.class);

    private final JwtDecoder applicationJwtDecoder;

    private final ClientRegistration clientRegistration;

    private final JwtDecoder clientRegistrationJwtDecoder;

    /**
     * Standard constructor with default application JWT Decoder and factory providing OpenID Connect JWT Decoder
     * @param applicationJwtDecoder Default JSON Web Token Decoder used when matching Issuer claim not found
     * @param jwtDecoderFactory OpenID Connect JWT Decoder Factory
     * @param clientRegistrationRepository OpenID Connect Client Registration Repository
     */
    public StandardIssuerJwtDecoder(
            final JwtDecoder applicationJwtDecoder,
            final JwtDecoderFactory<ClientRegistration> jwtDecoderFactory,
            final ClientRegistrationRepository clientRegistrationRepository
    ) {
        this.applicationJwtDecoder = Objects.requireNonNull(applicationJwtDecoder, "Application JWT Decoder required");
        this.clientRegistration = clientRegistrationRepository.findByRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty());
        if (clientRegistration == null) {
            logger.debug("OIDC Client Registration not configured for JWT Decoder");
            this.clientRegistrationJwtDecoder = null;
        } else {
            Objects.requireNonNull(jwtDecoderFactory, "JWT Decoder Factory required");
            this.clientRegistrationJwtDecoder = jwtDecoderFactory.createDecoder(clientRegistration);
        }
    }

    /**
     * Decode JSON Web Token using OpenID Connect Decoder for matched Issuer or default application JWT Decoder
     *
     * @param token JSON Web Token serialized and encoded
     * @return Decoded and validated JSON Web Token
     * @throws JwtException Thrown on malformed tokens or decoding failures
     */
    @Override
    public Jwt decode(final String token) throws JwtException {
        final Jwt decoded;

        if (clientRegistration == null) {
            decoded = applicationJwtDecoder.decode(token);
        } else {
            final JWT parsed = parse(token);
            final String tokenIssuer = getTokenIssuer(parsed);

            if (isIssuerRegistered(tokenIssuer)) {
                decoded = clientRegistrationJwtDecoder.decode(token);
            } else {
                decoded = applicationJwtDecoder.decode(token);
            }
        }

        return decoded;
    }

    private boolean isIssuerRegistered(final String tokenIssuer) {
        final boolean registered;

        if (clientRegistration == null) {
            registered = false;
        } else {
            final ClientRegistration.ProviderDetails providerDetails = clientRegistration.getProviderDetails();
            final String issuerUri = providerDetails.getIssuerUri();
            registered = issuerUri.equals(tokenIssuer);
        }

        return registered;
    }

    private String getTokenIssuer(final JWT parsed) {
        try {
            final JWTClaimsSet claimsSet = parsed.getJWTClaimsSet();
            final String issuer = claimsSet.getIssuer();
            if (issuer == null || issuer.isEmpty()) {
                throw new BadJwtException("Token Issuer claim not found");
            }
            return issuer;
        } catch (final Exception e) {
            throw new BadJwtException("Token Issuer parsing failed", e);
        }
    }

    private JWT parse(final String token) {
        if (token == null || token.isEmpty()) {
            throw new BadJwtException("Token not found");
        }

        try {
            final JWT parsed = JWTParser.parse(token);
            if (parsed instanceof PlainJWT) {
                throw new BadJwtException("Unsigned Token not supported");
            }
            return parsed;
        } catch (final Exception e) {
            throw new BadJwtException("Token parsing failed", e);
        }
    }
}
