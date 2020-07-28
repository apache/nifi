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
package org.apache.nifi.web.security.otp


import org.apache.nifi.web.security.token.OtpAuthenticationToken
import org.apache.nifi.web.security.util.CacheKey
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security
import java.util.concurrent.TimeUnit

@RunWith(JUnit4.class)
class TokenCacheTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TokenCache.class)

    private static final String andy = "alopresto"
    private static final String nathan = "ngough"
    private static final String matt = "mgilman"

    private static final int LONG_CACHE_EXPIRATION = 10
    private static final int SHORT_CACHE_EXPIRATION = 1

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    /**
     * Returns a simple "hash" of the provided principal (for test purposes, simply reverses the String).
     *
     * @param principal the token principal
     * @return the hashed token output
     */
    private static String hash(def principal) {
        principal.toString().reverse()
    }

    /**
     * Returns the {@link CacheKey} constructed from the provided token.
     *
     * @param token the authentication token
     * @return the cache key
     */
    private static CacheKey buildCacheKey(OtpAuthenticationToken token) {
        new CacheKey(hash(token.principal))
    }

    @Test
    void testShouldCheckIfContainsValue() throws Exception {
        // Arrange
        TokenCache tokenCache = new TokenCache("test tokens", LONG_CACHE_EXPIRATION, TimeUnit.SECONDS)

        OtpAuthenticationToken andyToken = new OtpAuthenticationToken(andy)
        OtpAuthenticationToken nathanToken = new OtpAuthenticationToken(nathan)

        tokenCache.put(buildCacheKey(andyToken), andy)
        tokenCache.put(buildCacheKey(nathanToken), nathan)

        logger.info(tokenCache.toString())

        // Act
        boolean containsAndyToken = tokenCache.containsValue(andy)
        boolean containsNathanToken = tokenCache.containsValue(nathan)
        boolean containsMattToken = tokenCache.containsValue(matt)

        // Assert
        assert containsAndyToken
        assert containsNathanToken
        assert !containsMattToken
    }

    @Test
    void testShouldGetKeyByValue() throws Exception {
        // Arrange
        TokenCache tokenCache = new TokenCache("test tokens", LONG_CACHE_EXPIRATION, TimeUnit.SECONDS)

        OtpAuthenticationToken andyToken = new OtpAuthenticationToken(andy)
        OtpAuthenticationToken nathanToken = new OtpAuthenticationToken(nathan)

        tokenCache.put(buildCacheKey(andyToken), andy)
        tokenCache.put(buildCacheKey(nathanToken), nathan)

        logger.info(tokenCache.toString())

        // Act
        CacheKey keyForAndyToken = tokenCache.getKeyForValue(andy)
        CacheKey keyForNathanToken = tokenCache.getKeyForValue(nathan)
        CacheKey keyForMattToken = tokenCache.getKeyForValue(matt)

        def tokens = [keyForAndyToken, keyForNathanToken, keyForMattToken]
        logger.info("Retrieved tokens: ${tokens}")

        // Assert
        assert keyForAndyToken.getKey() == hash(andyToken.principal)
        assert keyForNathanToken.getKey() == hash(nathanToken.principal)
        assert !keyForMattToken
    }

    @Test
    void testShouldNotGetKeyByValueAfterExpiration() throws Exception {
        // Arrange
        TokenCache tokenCache = new TokenCache("test tokens", SHORT_CACHE_EXPIRATION, TimeUnit.SECONDS)

        OtpAuthenticationToken andyToken = new OtpAuthenticationToken(andy)
        OtpAuthenticationToken nathanToken = new OtpAuthenticationToken(nathan)

        tokenCache.put(buildCacheKey(andyToken), andy)
        tokenCache.put(buildCacheKey(nathanToken), nathan)

        logger.info(tokenCache.toString())

        // Sleep to allow the cache entries to expire (was failing on Windows JDK 8 when only sleeping for 1 second)
        sleep(SHORT_CACHE_EXPIRATION * 2 * 1000)

        // Act
        CacheKey keyForAndyToken = tokenCache.getKeyForValue(andy)
        CacheKey keyForNathanToken = tokenCache.getKeyForValue(nathan)
        CacheKey keyForMattToken = tokenCache.getKeyForValue(matt)

        def tokens = [keyForAndyToken, keyForNathanToken, keyForMattToken]
        logger.info("Retrieved tokens: ${tokens}")

        // Assert
        assert !keyForAndyToken
        assert !keyForNathanToken
        assert !keyForMattToken
    }

    @Test
    void testShouldInvalidateSingleKey() throws Exception {
        // Arrange
        TokenCache tokenCache = new TokenCache("test tokens", LONG_CACHE_EXPIRATION, TimeUnit.SECONDS)

        OtpAuthenticationToken andyToken = new OtpAuthenticationToken(andy)
        OtpAuthenticationToken nathanToken = new OtpAuthenticationToken(nathan)
        OtpAuthenticationToken mattToken = new OtpAuthenticationToken(matt)

        CacheKey andyKey = buildCacheKey(andyToken)
        CacheKey nathanKey = buildCacheKey(nathanToken)
        CacheKey mattKey = buildCacheKey(mattToken)

        tokenCache.put(andyKey, andy)
        tokenCache.put(nathanKey, nathan)
        tokenCache.put(mattKey, matt)

        logger.info(tokenCache.toString())

        // Act
        tokenCache.invalidate(andyKey)

        // Assert
        assert !tokenCache.containsValue(andy)
        assert tokenCache.containsValue(nathan)
        assert tokenCache.containsValue(matt)
    }

    @Test
    void testShouldInvalidateMultipleKeys() throws Exception {
        // Arrange
        TokenCache tokenCache = new TokenCache("test tokens", LONG_CACHE_EXPIRATION, TimeUnit.SECONDS)

        OtpAuthenticationToken andyToken = new OtpAuthenticationToken(andy)
        OtpAuthenticationToken nathanToken = new OtpAuthenticationToken(nathan)
        OtpAuthenticationToken mattToken = new OtpAuthenticationToken(matt)

        CacheKey andyKey = buildCacheKey(andyToken)
        CacheKey nathanKey = buildCacheKey(nathanToken)
        CacheKey mattKey = buildCacheKey(mattToken)

        tokenCache.put(andyKey, andy)
        tokenCache.put(nathanKey, nathan)
        tokenCache.put(mattKey, matt)

        logger.info(tokenCache.toString())

        // Act
        tokenCache.invalidateAll([andyKey, nathanKey])

        // Assert
        assert !tokenCache.containsValue(andy)
        assert !tokenCache.containsValue(nathan)
        assert tokenCache.containsValue(matt)
    }

    @Test
    void testShouldInvalidateAll() throws Exception {
        // Arrange
        TokenCache tokenCache = new TokenCache("test tokens", LONG_CACHE_EXPIRATION, TimeUnit.SECONDS)

        OtpAuthenticationToken andyToken = new OtpAuthenticationToken(andy)
        OtpAuthenticationToken nathanToken = new OtpAuthenticationToken(nathan)
        OtpAuthenticationToken mattToken = new OtpAuthenticationToken(matt)

        CacheKey andyKey = buildCacheKey(andyToken)
        CacheKey nathanKey = buildCacheKey(nathanToken)
        CacheKey mattKey = buildCacheKey(mattToken)

        tokenCache.put(andyKey, andy)
        tokenCache.put(nathanKey, nathan)
        tokenCache.put(mattKey, matt)

        logger.info(tokenCache.toString())

        // Act
        tokenCache.invalidateAll()

        // Assert
        assert !tokenCache.containsValue(andy)
        assert !tokenCache.containsValue(nathan)
        assert !tokenCache.containsValue(matt)
    }
}
