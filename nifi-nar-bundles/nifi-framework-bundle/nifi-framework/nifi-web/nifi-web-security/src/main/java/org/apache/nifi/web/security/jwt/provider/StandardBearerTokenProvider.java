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
package org.apache.nifi.web.security.jwt.provider;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jwt.JWTClaimsSet;
import org.apache.nifi.web.security.jwt.jws.JwsSignerContainer;
import org.apache.nifi.web.security.jwt.jws.JwsSignerProvider;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.GrantedAuthority;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Standard Bearer Token Provider supports returning serialized and signed JSON Web Tokens
 */
public class StandardBearerTokenProvider implements BearerTokenProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardBearerTokenProvider.class);

    private static final Duration MAXIMUM_EXPIRATION = Duration.ofHours(12);

    private static final Duration MINIMUM_EXPIRATION = Duration.ofMinutes(1);

    private final JwsSignerProvider jwsSignerProvider;

    private final IssuerProvider issuerProvider;

    public StandardBearerTokenProvider(final JwsSignerProvider jwsSignerProvider, final IssuerProvider issuerProvider) {
        this.jwsSignerProvider = jwsSignerProvider;
        this.issuerProvider = issuerProvider;
    }

    /**
     * Get Signed JSON Web Token using Login Authentication Token
     *
     * @param loginAuthenticationToken Login Authentication Token
     * @return Serialized Signed JSON Web Token
     */
    @Override
    public String getBearerToken(final LoginAuthenticationToken loginAuthenticationToken) {
        Objects.requireNonNull(loginAuthenticationToken, "LoginAuthenticationToken required");
        final String subject = Objects.requireNonNull(loginAuthenticationToken.getPrincipal(), "Principal required").toString();
        final String username = loginAuthenticationToken.getName();

        final List<String> groups = getGroups(loginAuthenticationToken.getAuthorities());

        final URI issuerUri = issuerProvider.getIssuer();
        final String issuer = issuerUri.toString();

        final Date now = new Date();
        final Date expirationTime = getExpirationTime(loginAuthenticationToken);
        final JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .jwtID(UUID.randomUUID().toString())
                .subject(subject)
                .issuer(issuer)
                .audience(issuer)
                .notBeforeTime(now)
                .issueTime(now)
                .expirationTime(expirationTime)
                .claim(SupportedClaim.PREFERRED_USERNAME.getClaim(), username)
                .claim(SupportedClaim.GROUPS.getClaim(), groups)
                .build();
        return getSignedBearerToken(claims);
    }

    private Date getExpirationTime(final LoginAuthenticationToken loginAuthenticationToken) {
        Instant expiration = loginAuthenticationToken.getExpiration();

        final Instant maximumExpiration = Instant.now().plus(MAXIMUM_EXPIRATION);
        final Instant minimumExpiration = Instant.now().plus(MINIMUM_EXPIRATION);

        final String identity = loginAuthenticationToken.getName();
        if (expiration.isAfter(maximumExpiration)) {
            LOGGER.warn("Identity [{}] Token Expiration [{}] greater than maximum [{}]", identity, expiration, MAXIMUM_EXPIRATION);
            expiration = maximumExpiration;
        } else if (expiration.isBefore(minimumExpiration)) {
            LOGGER.warn("Identity [{}] Token Expiration [{}] less than minimum [{}]", identity, expiration, MINIMUM_EXPIRATION);
            expiration = minimumExpiration;
        }

        return Date.from(expiration);
    }

    private String getSignedBearerToken(final JWTClaimsSet claims) {
        final Date expirationTime = claims.getExpirationTime();
        final JwsSignerContainer jwsSignerContainer = jwsSignerProvider.getJwsSignerContainer(expirationTime.toInstant());

        final String keyIdentifier = jwsSignerContainer.getKeyIdentifier();
        final JWSAlgorithm algorithm = jwsSignerContainer.getJwsAlgorithm();
        final JWSHeader header = new JWSHeader.Builder(algorithm).keyID(keyIdentifier).build();
        final Payload payload = new Payload(claims.toJSONObject());
        final JWSObject jwsObject = new JWSObject(header, payload);

        final JWSSigner signer = jwsSignerContainer.getJwsSigner();
        try {
            jwsObject.sign(signer);
        } catch (final JOSEException e) {
            final String message = String.format("Signing Failed for Algorithm [%s] Key Identifier [%s]", algorithm, keyIdentifier);
            throw new IllegalArgumentException(message, e);
        }

        LOGGER.debug("Signed Bearer Token using Key [{}] for Subject [{}]", keyIdentifier, claims.getSubject());
        return jwsObject.serialize();
    }

    private List<String> getGroups(final Collection<? extends GrantedAuthority> authorities) {
        return authorities.stream().map(GrantedAuthority::getAuthority).collect(Collectors.toList());
    }
}
