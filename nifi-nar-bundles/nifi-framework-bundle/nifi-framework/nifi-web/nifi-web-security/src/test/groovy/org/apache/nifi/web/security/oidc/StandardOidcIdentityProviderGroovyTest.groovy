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
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata
import com.nimbusds.openid.connect.sdk.token.OIDCTokens
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm
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
            isOidcEnabled                 : false,
            getOidcDiscoveryUrl           : "https://localhost/oidc",
            isLoginIdentityProviderEnabled: false,
            isKnoxSsoEnabled              : false,
            getOidcConnectTimeout         : 1000,
            getOidcReadTimeout            : 1000,
            getOidcClientId               : "expected_client_id",
            getOidcClientSecret           : "expected_client_secret",
            getOidcClaimIdentifyingUser   : "username"
    ]

    // Mock collaborators
    private static NiFiProperties mockNiFiProperties
    private static JwtService mockJwtService = [:] as JwtService

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
        def mockNFP = combinedProps.collectEntries { String k, def v ->
            [k, { -> return v }]
        }
        mockNFP as NiFiProperties
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
                "iss"           : "https://accounts.google.com",
                "azp"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.googleusercontent.com",
                "aud"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.googleusercontent.com",
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
        assert msg =~ "An error occurred while invoking the UserInfo endpoint: The provided username and password were not correct"
    }

    @Test
    void testShouldConvertOIDCTokenToNiFiToken() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator(["getOidcClaimIdentifyingUser": "email"])

        OIDCTokenResponse mockResponse = mockOIDCTokenResponse()
        logger.info("OIDC Token Response: ${mockResponse.dump()}")

        // Act
        String nifiToken = soip.convertOIDCTokenToNiFiToken(mockResponse)
        logger.info("NiFi token: ${nifiToken}")

        // Assert

        // Split JWT into components and decode Base64 to JSON
        def (String headerB64, String payloadB64, String signatureB64) = nifiToken.tokenize("\\.")
        logger.info("Header: ${headerB64} | Payload: ${payloadB64} | Signature: ${signatureB64}")
        String headerJson = new String(Base64.decoder.decode(headerB64), "UTF-8")
        String payloadJson = new String(Base64.decoder.decode(payloadB64), "UTF-8")
        // String signatureJson = new String(Base64.decoder.decode(signatureB64), "UTF-8")

        // Parse JSON into objects
        def slurper = new JsonSlurper()
        def header = slurper.parseText(headerJson)
        logger.info("Header: ${header}")

        assert header.alg == "HS256"

        def payload = slurper.parseText(payloadJson)
        logger.info("Payload: ${payload}")

        assert payload.username == "person@nifi.apache.org"
        assert payload.keyId == 1
        assert payload.exp <= System.currentTimeMillis() + 10_000
    }

    @Test
    void testConvertOIDCTokenToNiFiTokenShouldHandleBlankIdentity() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator(["getOidcClaimIdentifyingUser": "non-existent-claim"])

        OIDCTokenResponse mockResponse = mockOIDCTokenResponse()
        logger.info("OIDC Token Response: ${mockResponse.dump()}")

        // Act
        String nifiToken = soip.convertOIDCTokenToNiFiToken(mockResponse)
        logger.info("NiFi token: ${nifiToken}")

        // Assert
        // Split JWT into components and decode Base64 to JSON
        def (String headerB64, String payloadB64, String signatureB64) = nifiToken.tokenize("\\.")
        logger.info("Header: ${headerB64} | Payload: ${payloadB64} | Signature: ${signatureB64}")
        String headerJson = new String(Base64.decoder.decode(headerB64), "UTF-8")
        String payloadJson = new String(Base64.decoder.decode(payloadB64), "UTF-8")
        // String signatureJson = new String(Base64.decoder.decode(signatureB64), "UTF-8")

        // Parse JSON into objects
        def slurper = new JsonSlurper()
        def header = slurper.parseText(headerJson)
        logger.info("Header: ${header}")

        assert header.alg == "HS256"

        def payload = slurper.parseText(payloadJson)
        logger.info("Payload: ${payload}")

        assert payload.username == "person@nifi.apache.org"
        assert payload.keyId == 1
        assert payload.exp <= System.currentTimeMillis() + 10_000
    }

    @Test
    void testConvertOIDCTokenToNiFiTokenShouldHandleBlankIdentityAndNoEmailClaim() {
        // Arrange
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator(["getOidcClaimIdentifyingUser": "non-existent-claim"])

        OIDCTokenResponse mockResponse = mockOIDCTokenResponse(["email": null])
        logger.info("OIDC Token Response: ${mockResponse.dump()}")

        // Act
        def msg = shouldFail {
            String nifiToken = soip.convertOIDCTokenToNiFiToken(mockResponse)
            logger.info("NiFi token: ${nifiToken}")
        }

        // Assert
        assert msg =~ "Connection refused|Remote host terminated the handshake"
    }

    @Test
    void testShouldAuthorizeClient() {
        // Arrange
        // Build ID Provider with mock token endpoint URI to make a connection
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator([:])

        // Mock the JWT
        def jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6Ik5pRmkgT0lEQyBVbml0IFRlc3RlciIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MzM5MDIyLCJpc3MiOiJuaWZpX3VuaXRfdGVzdF9hdXRob3JpdHkiLCJhdWQiOiJhbGwiLCJ1c2VybmFtZSI6Im9pZGNfdGVzdCIsImVtYWlsIjoib2lkY190ZXN0QG5pZmkuYXBhY2hlLm9yZyJ9.b4NIl0RONKdVLOH0D1eObdwAEX8qX-ExqB8KuKSZFLw"

        def responseBody = [id_token: jwt, access_token: "some.access.token", refresh_token: "some.refresh.token", token_type: "bearer"]
        HTTPRequest mockTokenRequest = mockHttpRequest(responseBody, 200, "HTTP OK")

        // Act
        def nifiToken = soip.authorizeClient(mockTokenRequest)
        logger.info("NiFi Token: ${nifiToken.dump()}")

        // Assert
        assert nifiToken
    }

    @Test
    void testAuthorizeClientShouldHandleError() {
        // Arrange
        // Build ID Provider with mock token endpoint URI to make a connection
        StandardOidcIdentityProvider soip = buildIdentityProviderWithMockTokenValidator([:])

        // Mock the JWT
        def jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6Ik5pRmkgT0lEQyBVbml0IFRlc3RlciIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MzM5MDIyLCJpc3MiOiJuaWZpX3VuaXRfdGVzdF9hdXRob3JpdHkiLCJhdWQiOiJhbGwiLCJ1c2VybmFtZSI6Im9pZGNfdGVzdCIsImVtYWlsIjoib2lkY190ZXN0QG5pZmkuYXBhY2hlLm9yZyJ9.b4NIl0RONKdVLOH0D1eObdwAEX8qX-ExqB8KuKSZFLw"

        def responseBody = [id_token: jwt, access_token: "some.access.token", refresh_token: "some.refresh.token", token_type: "bearer"]
        HTTPRequest mockTokenRequest = mockHttpRequest(responseBody, 500, "HTTP SERVER ERROR")

        // Act
        def msg = shouldFail(RuntimeException) {
            def nifiToken = soip.authorizeClient(mockTokenRequest)
            logger.info("NiFi token: ${nifiToken}")
        }

        // Assert
        assert msg =~ "An error occurred while invoking the Token endpoint: null"
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
        final Map<String, Object> claims = [
                "iss"           : "https://accounts.google.com",
                "azp"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.googleusercontent.com",
                "aud"           : "1013352044499-05pb1ssdfuihsdfsdsdfdi8r2vike88m.apps.googleusercontent.com",
                "sub"           : "10703475345439756345540",
                "email"         : "person@nifi.apache.org",
                "email_verified": "true",
                "at_hash"       : "JOGISUDHFiyGHDSFwV5Fah2A",
                "iat"           : 1590022674,
                "exp"           : 1590026274
        ] + additionalClaims

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

    class MockOIDCProviderMetadata extends OIDCProviderMetadata {

        MockOIDCProviderMetadata() {
            super([:] as Issuer, [SubjectType.PUBLIC] as List<SubjectType>, new URI("https://localhost"))
        }
    }
}
