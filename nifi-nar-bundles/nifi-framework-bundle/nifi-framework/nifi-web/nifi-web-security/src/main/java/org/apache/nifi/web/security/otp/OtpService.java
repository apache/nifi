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
package org.apache.nifi.web.security.otp;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.web.security.token.OtpAuthenticationToken;
import org.apache.nifi.web.security.util.CacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OtpService is a service for generating and verifying one time password tokens.
 */
public class OtpService {

    private static final Logger logger = LoggerFactory.getLogger(OtpService.class);

    private static final String HMAC_SHA256 = "HmacSHA256";

    // protected for testing purposes
    protected static final int MAX_CACHE_SOFT_LIMIT = 100;

    private final TokenCache downloadTokens;
    private final TokenCache uiExtensionTokens;

    /**
     * Creates a new OtpService with an expiration of 5 minutes.
     */
    public OtpService() {
        this(5, TimeUnit.MINUTES);
    }

    /**
     * Creates a new OtpService.
     *
     * @param duration                  The expiration duration
     * @param units                     The expiration units
     * @throws NullPointerException     If units is null
     * @throws IllegalArgumentException If duration is negative
     */
    public OtpService(final int duration, final TimeUnit units) {
        downloadTokens = new TokenCache("download tokens", duration, units);
        uiExtensionTokens = new TokenCache("UI extension tokens", duration, units);
    }

    /**
     * Generates a download token for the specified authentication.
     *
     * @param authenticationToken       The authentication
     * @return                          The one time use download token
     */
    public String generateDownloadToken(final OtpAuthenticationToken authenticationToken) {
        return generateToken(downloadTokens, authenticationToken);
    }

    /**
     * Gets the authenticated identity from the specified one time use download token. This method will not return null.
     *
     * @param token                     The one time use download token
     * @return                          The authenticated identity
     * @throws OtpAuthenticationException   When the specified token does not correspond to an authenticated identity
     */
    public String getAuthenticationFromDownloadToken(final String token) throws OtpAuthenticationException {
        return getAuthenticationFromToken(downloadTokens, token);
    }

    /**
     * Generates a UI extension token for the specified authentication.
     *
     * @param authenticationToken       The authentication
     * @return                          The one time use UI extension token
     */
    public String generateUiExtensionToken(final OtpAuthenticationToken authenticationToken) {
        return generateToken(uiExtensionTokens, authenticationToken);
    }

    /**
     * Gets the authenticated identity from the specified one time use UI extension token. This method will not return null.
     *
     * @param token                     The one time use UI extension token
     * @return                          The authenticated identity
     * @throws OtpAuthenticationException   When the specified token does not correspond to an authenticated identity
     */
    public String getAuthenticationFromUiExtensionToken(final String token) throws OtpAuthenticationException {
        return getAuthenticationFromToken(uiExtensionTokens, token);
    }

    /**
     * Generates a token and stores it in the specified cache.
     *
     * @param tokenCache                A cache that maps tokens to users
     * @param authenticationToken       The authentication
     * @return                          The one time use token
     */
    private String generateToken(final TokenCache tokenCache, final OtpAuthenticationToken authenticationToken) {
        final String userId = (String) authenticationToken.getPrincipal();

        // If the user has a token already, return it
        if(tokenCache.containsValue(userId)) {
            return (tokenCache.getKeyForValue(userId)).getKey();
        } else {
            // Otherwise, generate a token
            if (tokenCache.size() >= MAX_CACHE_SOFT_LIMIT) {
                throw new IllegalStateException("The maximum number of single use tokens have been issued.");
            }

            // Hash the authentication and build a cache key
            final CacheKey cacheKey = new CacheKey(hash(authenticationToken));

            // Store the token and user in the cache
            tokenCache.put(cacheKey, userId);

            // Return the token
            return cacheKey.getKey();
        }
    }

    /**
     * Gets the corresponding authentication for the specified one time use token. The specified token will be removed
     * from the token cache.
     *
     * @param tokenCache                A cache that maps tokens to users
     * @param token                     The one time use token
     * @return                          The authenticated identity
     */
    private String getAuthenticationFromToken(final TokenCache tokenCache, final String token) throws OtpAuthenticationException {
        final CacheKey cacheKey = new CacheKey(token);
        final String authenticatedUser = (String) tokenCache.getIfPresent(cacheKey);

        if (authenticatedUser == null) {
            throw new OtpAuthenticationException("Unable to validate the access token.");
        } else {
            tokenCache.invalidate(cacheKey);
        }

        return authenticatedUser;
    }

    /**
     * Hashes the specified authentication token. The resulting value will be used as the one time use token.
     *
     * @param authenticationToken   the authentication token
     * @return                      the one time use token
     */
    private String hash(final OtpAuthenticationToken authenticationToken) {
        try {
            // input is the user identity and timestamp
            final String input = authenticationToken.getName() + "-" + System.nanoTime();

            // create the secret using secure random
            final SecureRandom secureRandom = new SecureRandom();
            final byte[] randomBytes = new byte[32];
            secureRandom.nextBytes(randomBytes);
            final SecretKeySpec secret = new SecretKeySpec(randomBytes, HMAC_SHA256); // 256 bit

            // hash the input
            final Mac hmacSha256 = Mac.getInstance(HMAC_SHA256);
            hmacSha256.init(secret);
            final byte[] output = hmacSha256.doFinal(input.getBytes(StandardCharsets.UTF_8));

            // return the result as a base 64 string
            return Base64.encodeBase64URLSafeString(output);
        } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
            final String errorMessage = "There was an error generating the OTP";
            logger.error(errorMessage, e);
            throw new IllegalStateException("Unable to generate single use token.");
        }
    }
}
