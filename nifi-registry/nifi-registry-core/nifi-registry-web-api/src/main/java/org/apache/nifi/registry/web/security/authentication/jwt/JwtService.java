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
package org.apache.nifi.registry.web.security.authentication.jwt;

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
import org.apache.nifi.registry.security.authentication.AuthenticationResponse;
import org.apache.nifi.registry.security.key.Key;
import org.apache.nifi.registry.security.key.KeyService;
import org.apache.nifi.registry.web.security.authentication.exception.InvalidAuthenticationException;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO, look into replacing this JwtService service with Apache Licensed JJWT library
@Service
public class JwtService {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(JwtService.class);

    private static final SignatureAlgorithm SIGNATURE_ALGORITHM = SignatureAlgorithm.HS256;
    private static final String KEY_ID_CLAIM = "kid";
    private static final String USERNAME_CLAIM = "preferred_username";
    private static final Pattern tokenPattern = Pattern.compile("^Bearer (\\S*\\.\\S*\\.\\S*)$");
    public static final String AUTHORIZATION = "Authorization";

    private final KeyService keyService;

    @Autowired
    public JwtService(final KeyService keyService) {
        this.keyService = keyService;
    }

    public String getAuthenticationFromToken(final String base64EncodedToken) throws JwtException {
        // The library representations of the JWT should be kept internal to this service.
        try {
            final Jws<Claims> jws = parseTokenFromBase64EncodedString(base64EncodedToken);

            if (jws == null) {
                throw new JwtException("Unable to parse token");
            }

            // Additional validation that subject is present
            if (StringUtils.isEmpty(jws.getBody().getSubject())) {
                throw new JwtException("No subject available in token");
            }

            // TODO: Validate issuer against active IdentityProvider?
            if (StringUtils.isEmpty(jws.getBody().getIssuer())) {
                throw new JwtException("No issuer available in token");
            }
            return jws.getBody().getSubject();
        } catch (JwtException e) {
            logger.debug("The Base64 encoded JWT: " + base64EncodedToken);
            final String errorMessage = "There was an error validating the JWT";
            logger.error(errorMessage, e);
            throw e;
        }
    }

    private Jws<Claims> parseTokenFromBase64EncodedString(final String base64EncodedToken) throws JwtException {
        try {
            return Jwts.parser().setSigningKeyResolver(new SigningKeyResolverAdapter() {
                @Override
                public byte[] resolveSigningKeyBytes(JwsHeader header, Claims claims) {
                    final String identity = claims.getSubject();

                    // Get the key based on the key id in the claims
                    final String keyId = claims.get(KEY_ID_CLAIM, String.class);
                    final Key key = keyService.getKey(keyId);

                    // Ensure we were able to find a key that was previously issued by this key service for this user
                    if (key == null || key.getKey() == null) {
                        throw new UnsupportedJwtException("Unable to determine signing key for " + identity + " [kid: " + keyId + "]");
                    }

                    return key.getKey().getBytes(StandardCharsets.UTF_8);
                }
            }).parseClaimsJws(base64EncodedToken);
        } catch (final MalformedJwtException | UnsupportedJwtException | SignatureException | ExpiredJwtException | IllegalArgumentException e) {
            // TODO: Exercise all exceptions to ensure none leak key material to logs
            final String errorMessage = "Unable to validate the access token.";
            throw new JwtException(errorMessage, e);
        }
    }

    /**
     * Generates a signed JWT token from the provided IdentityProvider AuthenticationResponse
     *
     * @param authenticationResponse an instance issued by an IdentityProvider after identity claim has been verified as authentic
     * @return a signed JWT containing the user identity and the identity provider, Base64-encoded
     * @throws JwtException if there is a problem generating the signed token
     */
    public String generateSignedToken(final AuthenticationResponse authenticationResponse) throws JwtException {
        if (authenticationResponse == null) {
            throw new IllegalArgumentException("Cannot generate a JWT for a null authenticationResponse");
        }

        return generateSignedToken(
                authenticationResponse.getIdentity(),
                authenticationResponse.getUsername(),
                authenticationResponse.getIssuer(),
                authenticationResponse.getIssuer(),
                authenticationResponse.getExpiration());
    }

    public String generateSignedToken(String identity, String preferredUsername, String issuer, String audience, long expirationMillis) throws JwtException {

        if (identity == null || StringUtils.isEmpty(identity)) {
            String errorMessage = "Cannot generate a JWT for a token with an empty identity";
            errorMessage = issuer != null ? errorMessage + " issued by " + issuer + "." : ".";
            logger.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        // Compute expiration
        final Calendar now = Calendar.getInstance();
        long expirationMillisRelativeToNow = validateTokenExpiration(expirationMillis, identity);
        long expirationMillisSinceEpoch = now.getTimeInMillis() + expirationMillisRelativeToNow;
        final Calendar expiration = new Calendar.Builder().setInstant(expirationMillisSinceEpoch).build();

        try {
            // Get/create the key for this user
            final Key key = keyService.getOrCreateKey(identity);
            final byte[] keyBytes = key.getKey().getBytes(StandardCharsets.UTF_8);

            //logger.trace("Generating JWT for " + describe(authenticationResponse));

            // TODO: Implement "jti" claim with nonce to prevent replay attacks and allow blacklisting of revoked tokens
            // Build the token
            return Jwts.builder().setSubject(identity)
                    .setIssuer(issuer)
                    .setAudience(audience)
                    .claim(USERNAME_CLAIM, preferredUsername)
                    .claim(KEY_ID_CLAIM, key.getId())
                    .setIssuedAt(now.getTime())
                    .setExpiration(expiration.getTime())
                    .signWith(SIGNATURE_ALGORITHM, keyBytes).compact();
        } catch (NullPointerException e) {
            final String errorMessage = "Could not retrieve the signing key for JWT for " + identity;
            logger.error(errorMessage, e);
            throw new JwtException(errorMessage, e);
        }

    }

    public void logOut(String userIdentity) {
        if (userIdentity == null || userIdentity.isEmpty()) {
            throw new JwtException("Log out failed: The user identity was not present in the request token to log out user.");
        }

        try {
            keyService.deleteKey(userIdentity);
            logger.info("Deleted token from database.");
        } catch (Exception e) {
            logger.error("Unable to log out user: " + userIdentity + ". Failed to remove their token from database.");
            throw e;
        }
    }

    private static long validateTokenExpiration(long proposedTokenExpiration, String identity) {
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

    private static String describe(AuthenticationResponse authenticationResponse) {
        Calendar expirationTime = Calendar.getInstance();
        expirationTime.setTimeInMillis(authenticationResponse.getExpiration());
        long remainingTime = expirationTime.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
        dateFormat.setTimeZone(expirationTime.getTimeZone());
        String expirationTimeString = dateFormat.format(expirationTime.getTime());

        return new StringBuilder("LoginAuthenticationToken for ")
                .append(authenticationResponse.getUsername())
                .append(" issued by ")
                .append(authenticationResponse.getIssuer())
                .append(" expiring at ")
                .append(expirationTimeString)
                .append(" [")
                .append(authenticationResponse.getExpiration())
                .append(" ms, ")
                .append(remainingTime)
                .append(" ms remaining]")
                .toString();
    }

    public void logOutUsingAuthHeader(String authorizationHeader) {
        String base64EncodedToken = getTokenFromHeader(authorizationHeader);
        logOut(getAuthenticationFromToken(base64EncodedToken));
    }

    public static String getTokenFromHeader(String authenticationHeader) {
        Matcher matcher = tokenPattern.matcher(authenticationHeader);
        if(matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new InvalidAuthenticationException("JWT did not match expected pattern.");
        }
    }
}
