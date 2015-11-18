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
package org.apache.nifi.web.security.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.SigningKeyResolverAdapter;
import io.jsonwebtoken.UnsupportedJwtException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.KeyService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;

/**
 *
 */
public class JwtService {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(JwtService.class);

    private static final SignatureAlgorithm SIGNATURE_ALGORITHM = SignatureAlgorithm.HS256;
    private final static String AUTHORIZATION = "Authorization";

    private final KeyService keyService;

    public JwtService(final KeyService keyService) {
        this.keyService = keyService;
    }

    /**
     * Gets the Authentication by extracting a JWT token from the specified request.
     *
     * @param request Request to extract the token from
     * @return The user identifier from the token
     */
    public String getAuthentication(final HttpServletRequest request) {
        // TODO: Refactor request token extraction out of this service
        // extract/verify token from incoming request
        final String authorization = request.getHeader(AUTHORIZATION);
        final String base64EncodedToken = StringUtils.substringAfterLast(authorization, " ");

        return getAuthenticationFromToken(base64EncodedToken);
    }

    public String getAuthenticationFromToken(final String base64EncodedToken) {
        // The library representations of the JWT should be kept internal to this service.
        try {
            final Jws<Claims> jws = parseTokenFromBase64EncodedString(base64EncodedToken);
            return jws.getBody().getSubject();
        } catch (JwtException e) {
            logger.debug("The Base64 encoded JWT: " + base64EncodedToken);
            final String errorMessage = "There was an error parsing the Base64-encoded JWT";
            logger.error(errorMessage, e);
            throw new JwtException(errorMessage, e);
        }
    }

    private Jws<Claims> parseTokenFromBase64EncodedString(final String base64EncodedToken) throws JwtException {
        try {
            // TODO: Check algorithm for validity
            // TODO: Ensure signature verification occurs
            return Jwts.parser().setSigningKeyResolver(new SigningKeyResolverAdapter() {
                @Override
                public byte[] resolveSigningKeyBytes(JwsHeader header, Claims claims) {
                    final String identity = claims.getSubject();

                    // The key is unique per identity and should be retrieved from the key service
                    final String key = keyService.getKey(identity);

                    // Ensure we were able to find a key that was previously issued by this key service for this user
                    if (key == null) {
                        throw new UnsupportedJwtException("Unable to determine signing key for " + identity);
                    }

                    return key.getBytes(StandardCharsets.UTF_8);
                }
            }).parseClaimsJws(base64EncodedToken);
        } catch (final MalformedJwtException | UnsupportedJwtException | SignatureException | ExpiredJwtException | IllegalArgumentException | AdministrationException e) {
            // TODO: Exercise all exceptions to ensure none leak key material to logs
            final String errorMessage = "There was an error parsing the Base64-encoded JWT";
            logger.error(errorMessage, e);
            throw new JwtException(errorMessage, e);
        }
    }

    /**
     * Generates a signed JWT token from the provided (Spring Security) login authentication token.
     *
     * @param authenticationToken
     * @return a signed JWT containing the user identity and the identity provider, Base64-encoded
     * @throws JwtException
     */
    public String generateSignedToken(final LoginAuthenticationToken authenticationToken) throws JwtException {
        if (authenticationToken == null) {
            throw new IllegalArgumentException("Cannot generate a JWT for a null authentication token");
        }

        // Set expiration from the token
        final Calendar expiration = Calendar.getInstance();
        expiration.setTimeInMillis(authenticationToken.getExpiration());

        final Object principal = authenticationToken.getPrincipal();
        if (principal == null || StringUtils.isEmpty(principal.toString())) {
            final String errorMessage = "Cannot generate a JWT for a token with an empty identity issued by " + authenticationToken.getIssuer();
            logger.error(errorMessage);
            throw new JwtException(errorMessage);
        }

        // Create a JWT with the specified authentication
        final String identity = principal.toString();
        final String username = authenticationToken.getName();

        try {
            // Get/create the key for this user
            final String key = keyService.getOrCreateKey(identity);
            final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            logger.trace("Generating JWT for " + authenticationToken);

            // TODO: Implement "jti" claim with nonce to prevent replay attacks and allow blacklisting of revoked tokens

            // Build the token
            return Jwts.builder().setSubject(identity)
                    .setIssuer(authenticationToken.getIssuer())
                    .setAudience(authenticationToken.getIssuer())
                    .claim("preferred_username", username)
                    .setExpiration(expiration.getTime())
                    .setIssuedAt(Calendar.getInstance().getTime())
                    .signWith(SIGNATURE_ALGORITHM, keyBytes).compact();
        } catch (NullPointerException | AdministrationException e) {
            // TODO: Remove exception handling and pass through
            final String errorMessage = "Could not retrieve the signing key for JWT";
            logger.error(errorMessage, e);
            throw new JwtException(errorMessage, e);
        }
    }
}
