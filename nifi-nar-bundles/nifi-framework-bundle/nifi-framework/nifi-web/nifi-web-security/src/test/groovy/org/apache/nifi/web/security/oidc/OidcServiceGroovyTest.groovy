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
package org.apache.nifi.web.security.oidc

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod
import com.nimbusds.oauth2.sdk.id.Issuer
import com.nimbusds.openid.connect.sdk.SubjectType
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata
import org.apache.nifi.util.NiFiProperties
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNull

class OidcServiceGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(OidcServiceGroovyTest.class)

    private static final Map<String, Object> DEFAULT_NIFI_PROPERTIES = [
            "nifi.security.user.oidc.discovery.url"           : "https://localhost/oidc",
            "nifi.security.user.login.identity.provider"      : "provider",
            "nifi.security.user.knox.url"                     : "url",
            "nifi.security.user.oidc.connect.timeout"         : "1000",
            "nifi.security.user.oidc.read.timeout"            : "1000",
            "nifi.security.user.oidc.client.id"               : "expected_client_id",
            "nifi.security.user.oidc.client.secret"           : "expected_client_secret",
            "nifi.security.user.oidc.claim.identifying.user"  : "username",
            "nifi.security.user.oidc.preferred.jwsalgorithm"  : ""
    ]

    // Mock collaborators
    private static NiFiProperties mockNiFiProperties
    private static StandardOidcIdentityProvider soip

    private static final String MOCK_REQUEST_IDENTIFIER = "mock-request-identifier"
    private static final String MOCK_JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" +
            ".eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6Ik5pRmkgT0lEQyBVbml0IFRlc3Rlci" +
            "IsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MzM5MDIyLCJpc3MiOiJuaWZpX3Vua" +
            "XRfdGVzdF9hdXRob3JpdHkiLCJhdWQiOiJhbGwiLCJ1c2VybmFtZSI6Im9pZGNfdGVzd" +
            "CIsImVtYWlsIjoib2lkY190ZXN0QG5pZmkuYXBhY2hlLm9yZyJ9" +
            ".b4NIl0RONKdVLOH0D1eObdwAEX8qX-ExqB8KuKSZFLw"

    @BeforeAll
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        mockNiFiProperties = buildNiFiProperties()
        soip = new StandardOidcIdentityProvider(mockNiFiProperties)
    }

    private static NiFiProperties buildNiFiProperties(Map<String, Object> props = [:]) {
        def combinedProps = DEFAULT_NIFI_PROPERTIES + props
        new NiFiProperties(combinedProps)
    }

    @Test
    void testShouldStoreJwt() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockInitializedProvider([:])

        OidcService service = new OidcService(soip)

        // Expected JWT
        logger.info("EXPECTED_JWT: ${MOCK_JWT}")

        // Act
        service.storeJwt(MOCK_REQUEST_IDENTIFIER, MOCK_JWT)

        // Assert
        final String cachedJwt = service.getJwt(MOCK_REQUEST_IDENTIFIER)
        logger.info("Cached JWT: ${cachedJwt}")

        assertEquals(MOCK_JWT, cachedJwt)
    }

    @Test
    void testShouldGetJwt() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockInitializedProvider([:])

        OidcService service = new OidcService(soip)

        // Expected JWT
        logger.info("EXPECTED_JWT: ${MOCK_JWT}")

        // store the jwt
        service.storeJwt(MOCK_REQUEST_IDENTIFIER, MOCK_JWT)

        // Act
        final String retrievedJwt = service.getJwt(MOCK_REQUEST_IDENTIFIER)
        logger.info("Retrieved JWT: ${retrievedJwt}")

        // Assert
        assertEquals(MOCK_JWT, retrievedJwt)
    }

    @Test
    void testGetJwtShouldReturnNullWithExpiredDuration() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockInitializedProvider([:])

        final int DURATION = 500
        final TimeUnit EXPIRATION_UNITS = TimeUnit.MILLISECONDS
        OidcService service = new OidcService(soip, DURATION, EXPIRATION_UNITS)

        // Expected JWT
        logger.info("EXPECTED_JWT: ${MOCK_JWT}")

        // Store the jwt
        service.storeJwt(MOCK_REQUEST_IDENTIFIER, MOCK_JWT)

        // Put thread to sleep
        long millis = 1000
        Thread.sleep(millis)
        logger.info("Thread will sleep for: ${millis} ms")

        // Act
        final String retrievedJwt = service.getJwt(MOCK_REQUEST_IDENTIFIER)
        logger.info("Retrieved JWT: ${retrievedJwt}")

        // Assert
        assertNull(retrievedJwt)
    }

    private static StandardOidcIdentityProvider buildIdentityProviderWithMockInitializedProvider(Map<String, String> additionalProperties = [:]) {
        NiFiProperties mockNFP = buildNiFiProperties(additionalProperties)

        // Mock OIDC provider metadata
        Issuer mockIssuer = new Issuer("mockIssuer")
        URI mockURI = new URI("https://localhost/oidc")
        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)

        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockNFP) {
            @Override
            void initializeProvider() {
                soip.oidcProviderMetadata = metadata
                soip.oidcProviderMetadata["tokenEndpointAuthMethods"] = [ClientAuthenticationMethod.CLIENT_SECRET_BASIC]
                soip.oidcProviderMetadata["userInfoEndpointURI"] = new URI("https://localhost/oidc/token")
            }
        }
        soip
    }
}