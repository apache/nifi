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

import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jwt.JWT
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import com.nimbusds.oauth2.sdk.AuthorizationCode
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost
import com.nimbusds.oauth2.sdk.auth.Secret
import com.nimbusds.oauth2.sdk.http.HTTPRequest
import com.nimbusds.oauth2.sdk.http.HTTPResponse
import com.nimbusds.oauth2.sdk.id.ClientID
import com.nimbusds.oauth2.sdk.id.Issuer
import com.nimbusds.oauth2.sdk.token.AccessToken
import com.nimbusds.oauth2.sdk.token.BearerAccessToken
import com.nimbusds.oauth2.sdk.token.RefreshToken
import com.nimbusds.openid.connect.sdk.Nonce
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse
import com.nimbusds.openid.connect.sdk.SubjectType
import com.nimbusds.openid.connect.sdk.claims.AccessTokenHash
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata
import com.nimbusds.openid.connect.sdk.token.OIDCTokens
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator
import groovy.json.JsonOutput
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
import net.minidev.json.JSONObject
import org.apache.commons.codec.binary.Base64
import org.apache.nifi.admin.service.KeyService
import org.apache.nifi.key.Key
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.StringUtils
import org.apache.nifi.web.security.jwt.JwtService
import org.apache.nifi.web.security.token.LoginAuthenticationToken
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class StandardOidcIdentityProviderGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StandardOidcIdentityProviderGroovyTest.class)

    private static final Key SIGNING_KEY = new Key(id: 1, identity: "signingKey", key: "mock-signing-key-value")
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
    private static JwtService mockJwtService = [:] as JwtService

    private static final String MOCK_JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZ" +
            "SI6Ik5pRmkgT0lEQyBVbml0IFRlc3RlciIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MzM5MDIyLCJpc3MiOiJuaWZp" +
            "X3VuaXRfdGVzdF9hdXRob3JpdHkiLCJhdWQiOiJhbGwiLCJ1c2VybmFtZSI6Im9pZGNfdGVzdCIsImVtYWlsIjoib2lkY19" +
            "0ZXN0QG5pZmkuYXBhY2hlLm9yZyJ9.b4NIl0RONKdVLOH0D1eObdwAEX8qX-ExqB8KuKSZFLw"

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
        mockNiFiProperties = buildNiFiProperties()
    }

    @After
    void teardown() throws Exception {
    }

    private static NiFiProperties buildNiFiProperties(Map<String, Object> props = [:]) {
        def combinedProps = DEFAULT_NIFI_PROPERTIES + props
        new NiFiProperties(combinedProps)
    }

    private static JwtService buildJwtService() {
        def mockJS = new JwtService([:] as KeyService) {
            @Override
            String generateSignedToken(LoginAuthenticationToken lat) {
                signNiFiToken(lat)
            }
        }
        mockJS
    }

    private static String signNiFiToken(LoginAuthenticationToken lat) {
        String identity = "mockUser"
        String USERNAME_CLAIM = "username"
        String KEY_ID_CLAIM = "keyId"
        Calendar expiration = Calendar.getInstance()
        expiration.setTimeInMillis(System.currentTimeMillis() + 10_000)
        String username = lat.getName()

        return Jwts.builder().setSubject(identity)
                .setIssuer(lat.getIssuer())
                .setAudience(lat.getIssuer())
                .claim(USERNAME_CLAIM, username)
                .claim(KEY_ID_CLAIM, SIGNING_KEY.getId())
                .setExpiration(expiration.getTime())
                .setIssuedAt(Calendar.getInstance().getTime())
                .signWith(SignatureAlgorithm.HS256, SIGNING_KEY.key.getBytes("UTF-8")).compact()
    }

    @Test
    void testShouldGetAvailableClaims() {
        // Arrange
        final Map<String, String> EXPECTED_CLAIMS = [
                "iss"           : "https://accounts.issuer.com",
                "azp"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.usercontent.com",
                "aud"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.usercontent.com",
                "sub"           : "10703475345439756345540",
                "email"         : "person@nifi.apache.org",
                "email_verified": "true",
                "at_hash"       : "JOGISUDHFiyGHDSFwV5Fah2A",
                "iat"           : "1590022674",
                "exp"           : "1590026274",
                "empty_claim"   : ""
        ]

        final List<String> POPULATED_CLAIM_NAMES = EXPECTED_CLAIMS.findAll { k, v -> StringUtils.isNotBlank(v) }.keySet().sort()

        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(EXPECTED_CLAIMS)

        // Act
        def definedClaims = StandardOidcIdentityProvider.getAvailableClaims(mockJWTClaimsSet)
        logger.info("Defined claims: ${definedClaims}")

        // Assert
        assert definedClaims == POPULATED_CLAIM_NAMES
    }

    @Test
    void testShouldCreateClientAuthenticationFromPost() {
        // Arrange
        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockJwtService, mockNiFiProperties)

        Issuer mockIssuer = new Issuer("https://localhost/oidc")
        URI mockURI = new URI("https://localhost/oidc")
        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)

        soip.oidcProviderMetadata = metadata

        // Set Authorization Method
        soip.oidcProviderMetadata["tokenEndpointAuthMethods"] = [ClientAuthenticationMethod.CLIENT_SECRET_POST]
        final List<ClientAuthenticationMethod> mockAuthMethod = soip.oidcProviderMetadata["tokenEndpointAuthMethods"]
        logger.info("Provided Auth Method: ${mockAuthMethod}")

        // Define expected values
        final ClientID CLIENT_ID = new ClientID("expected_client_id")
        final Secret CLIENT_SECRET = new Secret("expected_client_secret")

        // Inject into OIP
        soip.clientId = CLIENT_ID
        soip.clientSecret = CLIENT_SECRET

        final ClientAuthentication EXPECTED_CLIENT_AUTHENTICATION = new ClientSecretPost(CLIENT_ID, CLIENT_SECRET)

        // Act
        def clientAuthentication = soip.createClientAuthentication()
        logger.info("Client Auth properties: ${clientAuthentication.getProperties()}")

        // Assert
        assert clientAuthentication.getClientID() == EXPECTED_CLIENT_AUTHENTICATION.getClientID()
        logger.info("Client secret: ${(clientAuthentication as ClientSecretPost).clientSecret.value}")
        assert ((ClientSecretPost) clientAuthentication).getClientSecret() == ((ClientSecretPost) EXPECTED_CLIENT_AUTHENTICATION).getClientSecret()
    }

    @Test
    void testShouldCreateClientAuthenticationFromBasic() {
        // Arrange
        // Mock collaborators
        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockJwtService, mockNiFiProperties)

        Issuer mockIssuer = new Issuer("https://localhost/oidc")
        URI mockURI = new URI("https://localhost/oidc")
        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)
        soip.oidcProviderMetadata = metadata

        // Set Auth Method
        soip.oidcProviderMetadata["tokenEndpointAuthMethods"] = [ClientAuthenticationMethod.CLIENT_SECRET_BASIC]
        final List<ClientAuthenticationMethod> mockAuthMethod = soip.oidcProviderMetadata["tokenEndpointAuthMethods"]
        logger.info("Provided Auth Method: ${mockAuthMethod}")

        // Define expected values
        final ClientID CLIENT_ID = new ClientID("expected_client_id")
        final Secret CLIENT_SECRET = new Secret("expected_client_secret")

        // Inject into OIP
        soip.clientId = CLIENT_ID
        soip.clientSecret = CLIENT_SECRET

        final ClientAuthentication EXPECTED_CLIENT_AUTHENTICATION = new ClientSecretBasic(CLIENT_ID, CLIENT_SECRET)

        // Act
        def clientAuthentication = soip.createClientAuthentication()
        logger.info("Client authentication properties: ${clientAuthentication.properties}")

        // Assert
        assert clientAuthentication.getClientID() == EXPECTED_CLIENT_AUTHENTICATION.getClientID()
        assert clientAuthentication.getMethod() == EXPECTED_CLIENT_AUTHENTICATION.getMethod()
        logger.info("Client secret: ${(clientAuthentication as ClientSecretBasic).clientSecret.value}")
        assert (clientAuthentication as ClientSecretBasic).getClientSecret() == EXPECTED_CLIENT_AUTHENTICATION.clientSecret
    }

    @Test
    void testShouldCreateTokenHTTPRequest() {
        // Arrange
        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockJwtService, mockNiFiProperties)

        // Mock AuthorizationGrant
        Issuer mockIssuer = new Issuer("https://localhost/oidc")
        URI mockURI = new URI("https://localhost/oidc")
        AuthorizationCode mockCode = new AuthorizationCode("ABCDE")
        def mockAuthGrant = new AuthorizationCodeGrant(mockCode, mockURI)

        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)
        soip.oidcProviderMetadata = metadata

        // Set OIDC Provider metadata attributes
        final ClientID CLIENT_ID = new ClientID("expected_client_id")
        final Secret CLIENT_SECRET = new Secret("expected_client_secret")

        // Inject into OIP
        soip.clientId = CLIENT_ID
        soip.clientSecret = CLIENT_SECRET
        soip.oidcProviderMetadata["tokenEndpointAuthMethods"] = [ClientAuthenticationMethod.CLIENT_SECRET_BASIC]
        soip.oidcProviderMetadata["tokenEndpointURI"] = new URI("https://localhost/token")

        // Mock ClientAuthentication
        def clientAuthentication = soip.createClientAuthentication()

        // Act
        def httpRequest = soip.createTokenHTTPRequest(mockAuthGrant, clientAuthentication)
        logger.info("HTTP Request: ${httpRequest.dump()}")
        logger.info("Query: ${URLDecoder.decode(httpRequest.query, "UTF-8")}")

        // Assert
        assert httpRequest.getMethod().name() == "POST"
        assert httpRequest.query =~ "code=${mockCode.value}"
        String encodedUri = URLEncoder.encode("https://localhost/oidc", "UTF-8")
        assert httpRequest.query =~ "redirect_uri=${encodedUri}&grant_type=authorization_code"
    }

    @Test
    void testShouldLookupIdentityInUserInfo() {
        // Arrange
        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockJwtService, mockNiFiProperties)

        Issuer mockIssuer = new Issuer("https://localhost/oidc")
        URI mockURI = new URI("https://localhost/oidc")

        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)
        soip.oidcProviderMetadata = metadata

        final String EXPECTED_IDENTITY = "my_username"

        def responseBody = [username: EXPECTED_IDENTITY, sub: "testSub"]
        HTTPRequest mockUserInfoRequest = mockHttpRequest(responseBody, 200, "HTTP OK")

        // Act
        String identity = soip.lookupIdentityInUserInfo(mockUserInfoRequest)
        logger.info("Identity: ${identity}")

        // Assert
        assert identity == EXPECTED_IDENTITY
    }

    @Test
    void testLookupIdentityUserInfoShouldHandleMissingIdentity() {
        // Arrange
        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockJwtService, mockNiFiProperties)

        Issuer mockIssuer = new Issuer("https://localhost/oidc")
        URI mockURI = new URI("https://localhost/oidc")

        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)
        soip.oidcProviderMetadata = metadata

        def responseBody = [username: "", sub: "testSub"]
        HTTPRequest mockUserInfoRequest = mockHttpRequest(responseBody, 200, "HTTP NO USER")

        // Act
        def msg = shouldFail(IllegalStateException) {
            String identity = soip.lookupIdentityInUserInfo(mockUserInfoRequest)
            logger.info("Identity: ${identity}")
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "Unable to extract identity from the UserInfo token using the claim 'username'."
    }

    @Test
    void testLookupIdentityUserInfoShouldHandle500() {
        // Arrange
        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockJwtService, mockNiFiProperties)

        Issuer mockIssuer = new Issuer("https://localhost/oidc")
        URI mockURI = new URI("https://localhost/oidc")

        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)
        soip.oidcProviderMetadata = metadata

        def errorBody = [error            : "Failure to authenticate",
                         error_description: "The provided username and password were not correct",
                         error_uri        : "https://localhost/oidc/error"]
        HTTPRequest mockUserInfoRequest = mockHttpRequest(errorBody, 500, "HTTP ERROR")

        // Act
        def msg = shouldFail(RuntimeException) {
            String identity = soip.lookupIdentityInUserInfo(mockUserInfoRequest)
            logger.info("Identity: ${identity}")
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "An error occurred while invoking the UserInfo endpoint: The provided username and password " +
                "were not correct"
    }

    @Test
    void testShouldConvertOIDCTokenToLoginAuthenticationToken() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator(["getOidcClaimIdentifyingUser": "email"])

        OIDCTokenResponse mockResponse = mockOIDCTokenResponse()
        logger.info("OIDC Token Response: ${mockResponse.dump()}")

        // Act
        final String loginToken = soip.convertOIDCTokenToLoginAuthenticationToken(mockResponse)
        logger.info("Login Authentication token: ${loginToken}")

        // Assert
        // Split ID Token into components
        def (String contents, String expiration) = loginToken.tokenize("\\[\\]")
        logger.info("Token contents: ${contents} | Expiration: ${expiration}")

        assert contents =~ "LoginAuthenticationToken for person@nifi\\.apache\\.org issued by https://accounts\\.issuer\\.com expiring at"

        // Assert expiration
        final String[] expList = expiration.split("\\s")
        final Long expLong = Long.parseLong(expList[0])
        assert expLong <= System.currentTimeMillis() + 10_000
    }

    @Test
    void testConvertOIDCTokenToLoginAuthenticationTokenShouldHandleBlankIdentity() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator(["getOidcClaimIdentifyingUser": "non-existent-claim"])

        OIDCTokenResponse mockResponse = mockOIDCTokenResponse()
        logger.info("OIDC Token Response: ${mockResponse.dump()}")

        // Act
        String loginToken = soip.convertOIDCTokenToLoginAuthenticationToken(mockResponse)
        logger.info("Login Authentication token: ${loginToken}")

        // Assert
        // Split ID Token into components
        def (String contents, String expiration) = loginToken.tokenize("\\[\\]")
        logger.info("Token contents: ${contents} | Expiration: ${expiration}")

        assert contents =~ "LoginAuthenticationToken for person@nifi\\.apache\\.org issued by https://accounts\\.issuer\\.com expiring at"

        // Get the expiration
        final ArrayList<String> expires = expiration.split("[\\D*]")
        final long exp = Long.parseLong(expires[0])
        logger.info("exp: ${exp} ms")

        assert exp <= System.currentTimeMillis() + 10_000
    }

    @Test
    void testConvertOIDCTokenToLoginAuthenticationTokenShouldHandleNoEmailClaimHasFallbackClaims() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator(
                ["nifi.security.user.oidc.claim.identifying.user": "email",
                 "nifi.security.user.oidc.fallback.claims.identifying.user": "upn" ])
        String expectedUpn = "xxx@aaddomain";

        OIDCTokenResponse mockResponse = mockOIDCTokenResponse(["email": null, "upn": expectedUpn])
        logger.info("OIDC Token Response with no email and upn: ${mockResponse.dump()}")

        String loginToken = soip.convertOIDCTokenToLoginAuthenticationToken(mockResponse)
        logger.info("NiFi token create with upn: ${loginToken}")
        // Assert
        // Split JWT into components and decode Base64 to JSON
        def (String contents, String expiration) = loginToken.tokenize("\\[\\]")
        logger.info("Token contents: ${contents} | Expiration: ${expiration}")
        assert contents =~ "LoginAuthenticationToken for ${expectedUpn} issued by https://accounts\\.issuer\\.com expiring at"
    }

    @Test
    void testConvertOIDCTokenToLoginAuthNTokenShouldHandleBlankIdentityAndNoEmailClaim() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator(["getOidcClaimIdentifyingUser": "non-existent-claim", "getOidcFallbackClaimsIdentifyingUser": [] ])

        OIDCTokenResponse mockResponse = mockOIDCTokenResponse(["email": null])
        logger.info("OIDC Token Response: ${mockResponse.dump()}")

        // Act
        def msg = shouldFail(IOException) {
            String loginAuthenticationToken = soip.convertOIDCTokenToLoginAuthenticationToken(mockResponse)
            logger.info("Login authentication token: ${loginAuthenticationToken}")
        }
        logger.expected(msg)
    }

    @Test
    void testShouldAuthorizeClientRequest() {
        // Arrange
        // Build ID Provider with mock token endpoint URI to make a connection
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator([:])

        def responseBody = [id_token: MOCK_JWT, access_token: "some.access.token", refresh_token: "some.refresh.token", token_type: "bearer"]
        HTTPRequest mockTokenRequest = mockHttpRequest(responseBody, 200, "HTTP OK")

        // Act
        def tokenResponse = soip.authorizeClientRequest(mockTokenRequest)
        logger.info("Token Response: ${tokenResponse.dump()}")

        // Assert
        assert tokenResponse
    }

    @Test
    void testAuthorizeClientRequestShouldHandleError() {
        // Arrange
        // Build ID Provider with mock token endpoint URI to make a connection
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator([:])

        def responseBody = [id_token: MOCK_JWT, access_token: "some.access.token", refresh_token: "some.refresh.token", token_type: "bearer"]
        HTTPRequest mockTokenRequest = mockHttpRequest(responseBody, 500, "HTTP SERVER ERROR")

        // Act
        def msg = shouldFail(RuntimeException) {
            def nifiToken = soip.authorizeClientRequest(mockTokenRequest)
            logger.info("NiFi token: ${nifiToken}")
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "An error occurred while invoking the Token endpoint: null"
    }

    @Test
    void testShouldGetAccessTokenString() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator()

        // Mock access tokens
        AccessToken mockAccessToken = new BearerAccessToken()
        RefreshToken mockRefreshToken = new RefreshToken()

        // Compute the access token hash
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.RS256
        AccessTokenHash EXPECTED_HASH = AccessTokenHash.compute(mockAccessToken, jwsAlgorithm)
        logger.info("Expected at_hash: ${EXPECTED_HASH}")

        // Create mock claims with at_hash
        Map<String, Object> mockClaims = (["at_hash": EXPECTED_HASH.toString()])

        // Create Claims Set
        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(mockClaims)

        // Create JWT
        JWT mockJwt = new PlainJWT(mockJWTClaimsSet)

        // Create OIDC Tokens
        OIDCTokens mockOidcTokens = new OIDCTokens(mockJwt, mockAccessToken, mockRefreshToken)

        // Create OIDC Token Response
        OIDCTokenResponse mockResponse = new OIDCTokenResponse(mockOidcTokens)

        // Act
        String accessTokenString = soip.getAccessTokenString(mockResponse)
        logger.info("Access token: ${accessTokenString}")

        // Assert
        assert accessTokenString
    }

    @Test
    void testShouldValidateAccessToken() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator()

        // Mock access tokens
        AccessToken mockAccessToken = new BearerAccessToken()
        logger.info("mock access token: ${mockAccessToken.toString()}")
        RefreshToken mockRefreshToken = new RefreshToken()

        // Compute the access token hash
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.RS256
        AccessTokenHash EXPECTED_HASH = AccessTokenHash.compute(mockAccessToken, jwsAlgorithm)
        logger.info("Expected at_hash: ${EXPECTED_HASH}")

        // Create mock claim
        final Map<String, Object> claims = mockClaims(["at_hash":EXPECTED_HASH.toString()])

        // Create Claims Set
        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(claims)

        // Create JWT
        JWT mockJwt = new PlainJWT(mockJWTClaimsSet)

        // Create OIDC Tokens
        OIDCTokens mockOidcTokens = new OIDCTokens(mockJwt, mockAccessToken, mockRefreshToken)
        logger.info("mock id tokens: ${mockOidcTokens.getIDToken().properties}")

        // Act
        String accessTokenString = soip.validateAccessToken(mockOidcTokens)
        logger.info("Access Token: ${accessTokenString}")

        // Assert
        assert accessTokenString == null
    }

    @Test
    void testValidateAccessTokenShouldHandleMismatchedATHash() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator()

        // Mock access tokens
        AccessToken mockAccessToken = new BearerAccessToken()
        RefreshToken mockRefreshToken = new RefreshToken()

        // Compute the access token hash
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.RS256
        AccessTokenHash expectedHash = AccessTokenHash.compute(mockAccessToken, jwsAlgorithm)
        logger.info("Expected at_hash: ${expectedHash}")

        // Create mock claim with incorrect 'at_hash'
        final Map<String, Object> claims = mockClaims()

        // Create Claims Set
        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(claims)

        // Create JWT
        JWT mockJwt = new PlainJWT(mockJWTClaimsSet)

        // Create OIDC Tokens
        OIDCTokens mockOidcTokens = new OIDCTokens(mockJwt, mockAccessToken, mockRefreshToken)

        // Act
        def msg = shouldFail(Exception) {
            soip.validateAccessToken(mockOidcTokens)
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "Unable to validate the Access Token: Access token hash \\(at_hash\\) mismatch"
    }

    @Test
    void testShouldGetIdTokenString() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator()

        // Mock access tokens
        AccessToken mockAccessToken = new BearerAccessToken()
        RefreshToken mockRefreshToken = new RefreshToken()

        // Create mock claim
        final Map<String, Object> claims = mockClaims()

        // Create Claims Set
        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(claims)

        // Create JWT
        JWT mockJwt = new PlainJWT(mockJWTClaimsSet)

        // Create OIDC Tokens
        OIDCTokens mockOidcTokens = new OIDCTokens(mockJwt, mockAccessToken, mockRefreshToken)

        final String EXPECTED_ID_TOKEN = mockOidcTokens.getIDTokenString()
        logger.info("EXPECTED_ID_TOKEN: ${EXPECTED_ID_TOKEN}")

        // Create OIDC Token Response
        OIDCTokenResponse mockResponse = new OIDCTokenResponse(mockOidcTokens)

        // Act
        final String idTokenString = soip.getIdTokenString(mockResponse)
        logger.info("ID Token: ${idTokenString}")

        // Assert
        assert idTokenString
        assert idTokenString == EXPECTED_ID_TOKEN

        // Assert that 'email:person@nifi.apache.org' is present
        def (String header, String payload) = idTokenString.split("\\.")
        final byte[] idTokenBytes = Base64.decodeBase64(payload)
        final String payloadString = new String(idTokenBytes, "UTF-8")
        logger.info(payloadString)

        assert payloadString =~ "\"email\":\"person@nifi\\.apache\\.org\""
    }

    @Test
    void testShouldValidateIdToken() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator()

        // Create mock claim
        final Map<String, Object> claims = mockClaims()

        // Create Claims Set
        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(claims)

        // Create JWT
        JWT mockJwt = new PlainJWT(mockJWTClaimsSet)

        // Act
        final IDTokenClaimsSet claimsSet = soip.validateIdToken(mockJwt)
        final String claimsSetString = claimsSet.toJSONObject().toString()
        logger.info("ID Token Claims Set: ${claimsSetString}")

        // Assert
        assert claimsSet
        assert claimsSetString =~ "\"email\":\"person@nifi\\.apache\\.org\""
    }

    @Test
    void testShouldGetOidcTokens() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator()

        // Mock access tokens
        AccessToken mockAccessToken = new BearerAccessToken()
        RefreshToken mockRefreshToken = new RefreshToken()

        // Create mock claim
        final Map<String, Object> claims = mockClaims()

        // Create Claims Set
        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(claims)

        // Create JWT
        JWT mockJwt = new PlainJWT(mockJWTClaimsSet)

        // Create OIDC Tokens
        OIDCTokens mockOidcTokens = new OIDCTokens(mockJwt, mockAccessToken, mockRefreshToken)

        final String EXPECTED_ID_TOKEN = mockOidcTokens.getIDTokenString()
        logger.info("EXPECTED_ID_TOKEN: ${EXPECTED_ID_TOKEN}")

        // Create OIDC Token Response
        OIDCTokenResponse mockResponse = new OIDCTokenResponse(mockOidcTokens)

        // Act
        final OIDCTokens oidcTokens = soip.getOidcTokens(mockResponse)
        logger.info("OIDC Tokens: ${oidcTokens.toJSONObject()}")

        // Assert
        assert oidcTokens

        // Assert ID Tokens match
        final JSONObject oidcJson = oidcTokens.toJSONObject()
        final String idToken = oidcJson["id_token"]
        logger.info("ID Token String: ${idToken}")
        assert idToken == EXPECTED_ID_TOKEN
    }

    private StandardOidcIdentityProvider buildIdentityProviderWithMockTokenValidator(Map<String, String> additionalProperties = [:]) {
        JwtService mockJS = buildJwtService()
        NiFiProperties mockNFP = buildNiFiProperties(additionalProperties)
        StandardOidcIdentityProvider soip = new StandardOidcIdentityProvider(mockJS, mockNFP)

        // Mock OIDC provider metadata
        Issuer mockIssuer = new Issuer("mockIssuer")
        URI mockURI = new URI("https://localhost/oidc")
        OIDCProviderMetadata metadata = new OIDCProviderMetadata(mockIssuer, [SubjectType.PUBLIC], mockURI)
        soip.oidcProviderMetadata = metadata

        // Set OIDC Provider metadata attributes
        final ClientID CLIENT_ID = new ClientID("expected_client_id")
        final Secret CLIENT_SECRET = new Secret("expected_client_secret")

        // Inject into OIP
        soip.clientId = CLIENT_ID
        soip.clientSecret = CLIENT_SECRET
        soip.oidcProviderMetadata["tokenEndpointAuthMethods"] = [ClientAuthenticationMethod.CLIENT_SECRET_BASIC]
        soip.oidcProviderMetadata["tokenEndpointURI"] = new URI("https://localhost/oidc/token")
        soip.oidcProviderMetadata["userInfoEndpointURI"] = new URI("https://localhost/oidc/userInfo")

        // Mock token validator
        IDTokenValidator mockTokenValidator = new IDTokenValidator(mockIssuer, CLIENT_ID) {
            @Override
            IDTokenClaimsSet validate(JWT jwt, Nonce nonce) {
                return new IDTokenClaimsSet(jwt.getJWTClaimsSet())
            }
        }
        soip.tokenValidator = mockTokenValidator
        soip
    }

    private OIDCTokenResponse mockOIDCTokenResponse(Map<String, Object> additionalClaims = [:]) {
        Map<String, Object> claims = mockClaims(additionalClaims)

        // Create Claims Set
        JWTClaimsSet mockJWTClaimsSet = new JWTClaimsSet(claims)

        // Create JWT
        JWT mockJwt = new PlainJWT(mockJWTClaimsSet)

        // Mock access tokens
        AccessToken mockAccessToken = new BearerAccessToken()
        RefreshToken mockRefreshToken = new RefreshToken()

        // Create OIDC Tokens
        OIDCTokens mockOidcTokens = new OIDCTokens(mockJwt, mockAccessToken, mockRefreshToken)

        // Create OIDC Token Response
        OIDCTokenResponse mockResponse = new OIDCTokenResponse(mockOidcTokens)
        mockResponse
    }

    private static Map<String, Object> mockClaims(Map<String, Object> additionalClaims = [:]) {
        final Map<String, Object> claims = [
                "iss"           : "https://accounts.issuer.com",
                "azp"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.usercontent.com",
                "aud"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.usercontent.com",
                "sub"           : "10703475345439756345540",
                "email"         : "person@nifi.apache.org",
                "email_verified": "true",
                "at_hash"       : "JOGISUDHFiyGHDSFwV5Fah2A",
                "iat"           : 1590022674,
                "exp"           : 1590026274
        ] + additionalClaims
        claims
    }

    /**
     * Forms an {@link HTTPRequest} object which returns a static response when {@code send( )} is called.
     *
     * @param body the JSON body in Map form
     * @param statusCode the HTTP status code
     * @param status the HTTP status message
     * @param headers an optional map of HTTP response headers
     * @param method the HTTP method to mock
     * @param url the endpoint URL
     * @return the static HTTP response
     */
    private static HTTPRequest mockHttpRequest(def body,
                                               int statusCode = 200,
                                               String status = "HTTP Response",
                                               Map<String, String> headers = [:],
                                               HTTPRequest.Method method = HTTPRequest.Method.GET,
                                               URL url = new URL("https://localhost/oidc")) {
        new HTTPRequest(method, url) {
            HTTPResponse send() {
                HTTPResponse mockResponse = new HTTPResponse(statusCode)
                mockResponse.setStatusMessage(status)
                (["Content-Type": "application/json"] + headers).each { String h, String v -> mockResponse.setHeader(h, v) }
                def responseBody = body
                mockResponse.setContent(JsonOutput.toJson(responseBody))
                mockResponse
            }
        }
    }
}
