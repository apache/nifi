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

import io.jsonwebtoken.JwtException;
import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.KeyService;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.key.Key;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.nifi.util.NiFiProperties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX;
import static org.apache.nifi.util.NiFiProperties.SECURITY_IDENTITY_MAPPING_VALUE_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(JwtServiceTest.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * These constant strings were generated using the tool at http://jwt.io
     */
    private static final String VALID_SIGNED_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiJhbG9wcmVzdG8iLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRl"
            + "ciIsImF1ZCI6Ik1vY2tJZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZ"
            + "XJuYW1lIjoiYWxvcHJlc3RvIiwia2lkIjoxLCJleHAiOjI0NDc4MDg3NjEsIm"
            + "lhdCI6MTQ0NzgwODcwMX0.r6aGZ6FNNYMOpcXW8BK2VYaQeX1uO0Aw1KJfjB3Q1DU";

    // This token has an empty subject field
    private static final String INVALID_SIGNED_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiIiLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsImF1ZCI6Ik1vY2tJZG"
            + "VudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxvcHJlc3RvI"
            + "iwia2lkIjoxLCJleHAiOjI0NDc4MDg3NjEsImlhdCI6MTQ0NzgwODcwMX0"
            + ".x_1p2M6E0vwWHWMujIUnSL3GkFoDqqICllRxo2SMNaw";

    private static final String VALID_UNSIGNED_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiJhbG9wcmVzdG8iLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsImF1ZC"
            + "I6Ik1vY2tJZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxvcHJl"
            + "c3RvIiwia2lkIjoiYWxvcHJlc3RvIiwiZXhwIjoyNDQ3ODA4NzYxLCJpYXQiOjE0NDc4MDg3MDF9";

    // This token has an empty subject field
    private static final String INVALID_UNSIGNED_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiIiLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsImF1ZCI6Ik1vY2tJZGVu"
            + "dGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxvcHJlc3RvIiwia2lkIjoi"
            + "YWxvcHJlc3RvIiwiZXhwIjoyNDQ3ODA4NzYxLCJpYXQiOjE0NDc4MDg3MDF9";

    // Algorithm field is "none"
    private static final String VALID_MALSIGNED_TOKEN = "eyJhbGciOiJub25lIn0"
            + ".eyJzdWIiOiJhbG9wcmVzdG8iLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsImF1ZC"
            + "I6Ik1vY2tJZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxvcHJl"
            + "c3RvIiwia2lkIjoiYWxvcHJlc3RvIiwiZXhwIjoxNDQ3ODA4NzYxLCJpYXQiOjE0NDc4MDg3MDF9"
            + ".mPO_wMNMl_zjMNevhNvUoXbSJ9Kx6jAe5OxDIAzKQbI";

    // Algorithm field is "none" and no signature is present
    private static final String VALID_MALSIGNED_NO_SIG_TOKEN = "eyJhbGciOiJub25lIn0"
            + ".eyJzdWIiOiJhbG9wcmVzdG8iLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsImF1ZCI6Ik1vY"
            + "2tJZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxvcHJlc3RvIiwia2lkIj"
            + "oiYWxvcHJlc3RvIiwiZXhwIjoyNDQ3ODA4NzYxLCJpYXQiOjE0NDc4MDg3MDF9.";

    // This token has an empty subject field
    private static final String INVALID_MALSIGNED_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiIiLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsImF1ZCI6Ik1vY2tJZGVud"
            + "Gl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxvcHJlc3RvIiwia2lkIjoiYW"
            + "xvcHJlc3RvIiwiZXhwIjoxNDQ3ODA4NzYxLCJpYXQiOjE0NDc4MDg3MDF9.WAwmUY4KHKV2oARNodkqDkbZsfRXGZfD2Ccy64GX9QF";

    // This token is signed but expired
    private static final String EXPIRED_SIGNED_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiIiLCJpc3MiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsImF1ZCI6Ik"
            + "1vY2tJZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxvc"
            + "HJlc3RvIiwia2lkIjoxLCJleHAiOjE0NDc4MDg3NjEsImlhdCI6MTQ0NzgwODcw"
            + "MX0.ZPDIhNKuL89vTGXcuztOYaGifwcrQy_gid4j8Sspmto";

    // Subject is "mgilman" but signed with "alopresto" key
    private static final String IMPOSTER_SIGNED_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiJtZ2lsbWFuIiwiaXNzIjoiTW9ja0lkZW50aXR5UHJvdmlkZXIiLCJ"
            + "hdWQiOiJNb2NrSWRlbnRpdHlQcm92aWRlciIsInByZWZlcnJlZF91c2VybmFtZSI"
            + "6ImFsb3ByZXN0byIsImtpZCI6MSwiZXhwIjoyNDQ3ODA4NzYxLCJpYXQiOjE0NDc"
            + "4MDg3MDF9.aw5OAvLTnb_sHmSQOQzW-A7NImiZgXJ2ngbbNL2Ymkc";

    // Issuer field is set to unknown provider
    private static final String UNKNOWN_ISSUER_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiJhbG9wcmVzdG8iLCJpc3MiOiJVbmtub3duSWRlbnRpdHlQcm92aWRlciIsIm"
            + "F1ZCI6Ik1vY2tJZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxv"
            + "cHJlc3RvIiwia2lkIjoiYWxvcHJlc3RvIiwiZXhwIjoyNDQ3ODA4NzYxLCJpYXQiOjE0NDc4MDg3MDF9"
            + ".SAd9tyNwSaijWet9wvAWSNmpxmPSK4XQuLx7h3ARqBo";

    // Issuer field is absent
    private static final String NO_ISSUER_TOKEN = "eyJhbGciOiJIUzI1NiJ9"
            + ".eyJzdWIiOiJhbG9wcmVzdG8iLCJhdWQiOiJNb2NrSWRlbnRpdHlQcm92a"
            + "WRlciIsInByZWZlcnJlZF91c2VybmFtZSI6ImFsb3ByZXN0byIsImtpZCI"
            + "6MSwiZXhwIjoyNDQ3ODA4NzYxLCJpYXQiOjE0NDc4MDg3MDF9.6kDjDanA"
            + "g0NQDb3C8FmgbBAYDoIfMAEkF4WMVALsbJA";

    private static final String KERBEROS_PROVIDER_TOKEN = "eyJhbGciOiJIUzI1NiJ9" +
            ".eyJzdWIiOiJuaWZpYWRtaW5AbmlmaS5hcGFjaGUub3JnIiwiaXNzIjoiS2VyYmVyb" +
            "3NQcm92aWRlciIsImF1ZCI6IktlcmJlcm9zUHJvdmlkZXIiLCJwcmVmZXJyZWRfdXN" +
            "lcm5hbWUiOiJuaWZpYWRtaW5AbmlmaS5hcGFjaGUub3JnIiwia2lkIjo2LCJleHAiO" +
            "jE2OTI0NTQ2NjcsImlhdCI6MTU5MjQxMTQ2N30.Mmnx6ssdjQ5_5VVRiyPWU60Oegc" +
            "NdhWezaKKNK48Mew";

    private static final String DEFAULT_HEADER = "{\"alg\":\"HS256\"}";
    private static final String DEFAULT_IDENTITY = "alopresto";
    private static final String REALMED_KERBEROS_IDENTITY = "nifiadmin@nifi.apache.org";
    private static final String KERBEROS_IDENTITY = "nifiadmin";

    private static final String TOKEN_DELIMITER = ".";

    private static final String HMAC_SECRET = "test_hmac_shared_secret";

    private static List<IdentityMapping> identityMappings;

    private KeyService mockKeyService;
    private KeyService testKeyService;

    // Class under test
    private JwtService jwtService;
    private JwtService jwtServiceUsingTestKeyService;

    public static String generateHS256Token(String rawHeader, String rawPayload, boolean isValid, boolean isSigned) {
        return generateHS256Token(rawHeader, rawPayload, HMAC_SECRET, isValid, isSigned);
    }

    private static String generateHS256Token(String rawHeader, String rawPayload, String hmacSecret, boolean isValid,
            boolean isSigned) {
        try {
            logger.info("Generating token for " + rawHeader + " + " + rawPayload);

            String base64Header = Base64.encodeBase64URLSafeString(rawHeader.getBytes(StandardCharsets.UTF_8));
            String base64Payload = Base64.encodeBase64URLSafeString(rawPayload.getBytes(StandardCharsets.UTF_8));
            // TODO: Support valid/invalid manipulation

            final String body = base64Header + TOKEN_DELIMITER + base64Payload;

            String signature = generateHMAC(hmacSecret, body);

            return body + TOKEN_DELIMITER + signature;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            final String errorMessage = "Could not generate the token";
            logger.error(errorMessage, e);
            fail(errorMessage);
            return null;
        }
    }

    private static String generateHMAC(String hmacSecret, String body) throws NoSuchAlgorithmException,
            InvalidKeyException {
        Mac hmacSHA256 = Mac.getInstance("HmacSHA256");
        SecretKeySpec secret_key = new SecretKeySpec(hmacSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        hmacSHA256.init(secret_key);
        return Base64.encodeBase64URLSafeString(hmacSHA256.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    @Before
    public void setUp() throws Exception {
        final Key key = new Key();
        key.setId(1);
        key.setIdentity(DEFAULT_IDENTITY);
        key.setKey(HMAC_SECRET);

        Answer<Key> keyAnswer = new Answer<Key>() {
            Key answerKey = key;
            @Override
            public Key answer(InvocationOnMock invocation) throws Throwable {
                if(invocation.getMethod().equals(KeyService.class.getMethod("deleteKey", Integer.class))) {
                    answerKey = null;
                }
                return answerKey;
            }
        };

        StandardNiFiUser nifiUser = mock(StandardNiFiUser.class);
        when(nifiUser.getIdentity()).thenReturn(DEFAULT_IDENTITY);
        NiFiUserDetails nifiUserDetails = mock(NiFiUserDetails.class);
        when(nifiUserDetails.getNiFiUser()).thenReturn(nifiUser);

        Authentication authentication = mock(Authentication.class);
        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        SecurityContextHolder.setContext(securityContext);
        when(SecurityContextHolder.getContext().getAuthentication().getPrincipal()).thenReturn(nifiUserDetails);

        mockKeyService = mock(KeyService.class);
        when(mockKeyService.getKey(anyInt())).thenAnswer(keyAnswer);
        when(mockKeyService.getOrCreateKey(anyString())).thenReturn(key);
        doAnswer(keyAnswer).when(mockKeyService).deleteKey(anyInt());

        jwtService = new JwtService(mockKeyService);
        jwtServiceUsingTestKeyService = new JwtService(new TestKeyService());

        Properties props = new Properties();
        props.setProperty(SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX+"kerb",  "^(.*?)@(.*?)$");
        props.setProperty(SECURITY_IDENTITY_MAPPING_VALUE_PREFIX+"kerb", "$1");
        identityMappings = IdentityMappingUtil.getIdentityMappings(new NiFiProperties(props));
    }

    @After
    public void tearDown() throws Exception {
        jwtService = null;
    }

    @Test
    public void testShouldGetAuthenticationForValidToken() throws Exception {
        // Arrange
        String token = VALID_SIGNED_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        assertEquals("Identity", DEFAULT_IDENTITY, identity);
    }

    @Test
    public void testShouldGetAuthenticationForValidKerberosToken() throws Exception {
        // Arrange
        String token = KERBEROS_PROVIDER_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        assertEquals("Identity", REALMED_KERBEROS_IDENTITY, identity);
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForInvalidToken() throws Exception {
        // Arrange
        String token = INVALID_SIGNED_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForEmptyToken() throws Exception {
        // Arrange
        String token = "";

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForUnsignedToken() throws Exception {
        // Arrange
        String token = VALID_UNSIGNED_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForMalsignedToken() throws Exception {
        // Arrange
        String token = VALID_MALSIGNED_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForTokenWithWrongAlgorithm() throws Exception {
        // Arrange
        String token = VALID_MALSIGNED_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForTokenWithWrongAlgorithmAndNoSignature() throws Exception {
        // Arrange
        String token = VALID_MALSIGNED_NO_SIG_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Ignore("Not yet implemented")
    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForTokenFromUnknownIdentityProvider() throws Exception {
        // Arrange
        String token = UNKNOWN_ISSUER_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForTokenFromEmptyIdentityProvider() throws Exception {
        // Arrange
        String token = NO_ISSUER_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForExpiredToken() throws Exception {
        // Arrange
        String token = EXPIRED_SIGNED_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGetAuthenticationForImposterToken() throws Exception {
        // Arrange
        String token = IMPOSTER_SIGNED_TOKEN;

        // Act
        String identity = jwtService.getAuthenticationFromToken(token);
        logger.info("Extracted identity: " + identity);

        // Assert
        // Should fail
    }

    @Test
    public void testShouldGenerateSignedToken() throws Exception {
        // Arrange

        // Token expires in 60 seconds
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken("alopresto",
                EXPIRATION_MILLIS,
                "MockIdentityProvider");
        logger.info("Generating token for " + loginAuthenticationToken);

        final String EXPECTED_HEADER = DEFAULT_HEADER;

        // Convert the expiration time from ms to s
        final long TOKEN_EXPIRATION_SEC = (long) (loginAuthenticationToken.getExpiration() / 1000.0);

        // Act
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        logger.info("Generated JWT: " + token);

        // Run after the SUT generates the token to ensure the same issued at time
        // Split the token, decode the middle section, and form a new String
        final String DECODED_PAYLOAD = new String(Base64.decodeBase64(token.split("\\.")[1].getBytes()));
        final long ISSUED_AT_SEC = Long.valueOf(DECODED_PAYLOAD.substring(DECODED_PAYLOAD.lastIndexOf(":") + 1,
                DECODED_PAYLOAD.length() - 1));
        logger.trace("Actual token was issued at " + ISSUED_AT_SEC);

        // Always use LinkedHashMap to enforce order of the signingKeys because the signature depends on order
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("sub", "alopresto");
        claims.put("iss", "MockIdentityProvider");
        claims.put("aud", "MockIdentityProvider");
        claims.put("preferred_username", "alopresto");
        claims.put("kid", 1);
        claims.put("exp", TOKEN_EXPIRATION_SEC);
        claims.put("iat", ISSUED_AT_SEC);
        logger.trace("JSON Object to String: " + new JSONObject(claims).toString());

        final String EXPECTED_PAYLOAD = new JSONObject(claims).toString();
        final String EXPECTED_TOKEN_STRING = generateHS256Token(EXPECTED_HEADER, EXPECTED_PAYLOAD, true, true);
        logger.info("Expected JWT: " + EXPECTED_TOKEN_STRING);

        // Assert
        assertEquals("JWT token", EXPECTED_TOKEN_STRING, token);
    }

    @Test
    public void testShouldGenerateSignedTokenWithURLEncodedIssuer() throws Exception {
        // Arrange

        // Token expires in 60 seconds
        final int EXPIRATION_MILLIS = 60000;
        final String rawIssuer = "https://accounts.google.com/o/saml2?idpid=acode";
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken("alopresto", EXPIRATION_MILLIS, rawIssuer);
        logger.info("Generating token for " + loginAuthenticationToken);

        final String EXPECTED_HEADER = DEFAULT_HEADER;

        // Convert the expiration time from ms to s
        final long TOKEN_EXPIRATION_SEC = (long) (loginAuthenticationToken.getExpiration() / 1000.0);

        // Act
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        logger.info("Generated JWT: " + token);

        // Run after the SUT generates the token to ensure the same issued at time
        // Split the token, decode the middle section, and form a new String
        final String DECODED_PAYLOAD = new String(Base64.decodeBase64(token.split("\\.")[1].getBytes()));
        final long ISSUED_AT_SEC = Long.valueOf(DECODED_PAYLOAD.substring(DECODED_PAYLOAD.lastIndexOf(":") + 1,
                DECODED_PAYLOAD.length() - 1));
        logger.trace("Actual token was issued at " + ISSUED_AT_SEC);

        // Always use LinkedHashMap to enforce order of the signingKeys because the signature depends on order
        final String encodedIssuer = URLEncoder.encode(rawIssuer, "UTF-8");

        final Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("sub", "alopresto");
        claims.put("iss", encodedIssuer);
        claims.put("aud", encodedIssuer);
        claims.put("preferred_username", "alopresto");
        claims.put("kid", 1);
        claims.put("exp", TOKEN_EXPIRATION_SEC);
        claims.put("iat", ISSUED_AT_SEC);
        logger.trace("JSON Object to String: " + new JSONObject(claims).toString());

        final String EXPECTED_PAYLOAD = new JSONObject(claims).toString();
        final String EXPECTED_TOKEN_STRING = generateHS256Token(EXPECTED_HEADER, EXPECTED_PAYLOAD, true, true);
        logger.info("Expected JWT: " + EXPECTED_TOKEN_STRING);

        // Assert
        assertEquals("JWT token", EXPECTED_TOKEN_STRING, token);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testShouldNotGenerateTokenWithNullAuthenticationToken() throws Exception {
        // Arrange
        LoginAuthenticationToken nullLoginAuthenticationToken = null;
        logger.info("Generating token for " + nullLoginAuthenticationToken);

        // Act
        jwtService.generateSignedToken(nullLoginAuthenticationToken);

        // Assert
        // Should throw exception
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGenerateTokenWithEmptyIdentity() throws Exception {
        // Arrange
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken emptyIdentityLoginAuthenticationToken = new LoginAuthenticationToken("",
                EXPIRATION_MILLIS, "MockIdentityProvider");
        logger.info("Generating token for " + emptyIdentityLoginAuthenticationToken);

        // Act
        jwtService.generateSignedToken(emptyIdentityLoginAuthenticationToken);

        // Assert
        // Should throw exception
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGenerateTokenWithNullIdentity() throws Exception {
        // Arrange
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken nullIdentityLoginAuthenticationToken = new LoginAuthenticationToken(null,
                EXPIRATION_MILLIS, "MockIdentityProvider");
        logger.info("Generating token for " + nullIdentityLoginAuthenticationToken);

        // Act
        jwtService.generateSignedToken(nullIdentityLoginAuthenticationToken);

        // Assert
        // Should throw exception
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGenerateTokenWithMissingKey() throws Exception {
        // Arrange
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(DEFAULT_IDENTITY,
                EXPIRATION_MILLIS,
                "MockIdentityProvider");
        logger.info("Generating token for " + loginAuthenticationToken);

        // Set up the bad key service
        KeyService missingKeyService = mock(KeyService.class);
        when(missingKeyService.getOrCreateKey(anyString())).thenThrow(new AdministrationException("Could not find a "
                + "key for that user"));
        jwtService = new JwtService(missingKeyService);

        // Act
        jwtService.generateSignedToken(loginAuthenticationToken);

        // Assert
        // Should throw exception
    }

    @Test
    public void testShouldLogOutUser() throws Exception {
        // Arrange
        expectedException.expect(JwtException.class);
        expectedException.expectMessage("Unable to validate the access token.");

        // Token expires in 60 seconds
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(DEFAULT_IDENTITY,
                EXPIRATION_MILLIS,
                "MockIdentityProvider");
        logger.info("Generating token for " + loginAuthenticationToken);

        // Act
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        logger.info("Generated JWT: " + token);
        logger.info("Validating token...");
        String authID = jwtService.getAuthenticationFromToken(token);
        assertEquals(DEFAULT_IDENTITY, authID);
        logger.info("Token was valid");
        logger.info("Logging out user: " + authID);
        jwtService.logOut(token);
        logger.info("Logged out user: " + authID);
        logger.info("Checking that token is now invalid...");
        jwtService.getAuthenticationFromToken(token);

        // Assert
        // Should throw exception when user is not found
    }

    @Test
    public void testLogoutWhenAuthTokenIsEmptyShouldThrowError() throws Exception {
        // Arrange
        expectedException.expect(JwtException.class);
        expectedException.expectMessage("Unable to validate the access token.");

        // Act
        jwtService.logOut(null);

        // Assert
        // Should throw exception when authorization header is null
    }

    @Test
    public void testShouldLogOutKerberosUser() throws Exception {
        // Arrange

        expectedException.expect(JwtException.class);
        expectedException.expectMessage("Unable to validate the access token.");

        // Token expires in 60 seconds
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(KERBEROS_IDENTITY,
                EXPIRATION_MILLIS,
                "MockIdentityProvider");
        logger.info("Generating token for " + loginAuthenticationToken);

        // Act
        String token = jwtServiceUsingTestKeyService.generateSignedToken(loginAuthenticationToken);
        logger.info("Generated JWT: " + token);
        logger.info("Validating token...");
        String authID = jwtServiceUsingTestKeyService.getAuthenticationFromToken(token);
        logger.info("Token was valid, unmapped user identity was: " + authID);
        assertEquals(KERBEROS_IDENTITY, authID);
        logger.info("Using identity mappings " + Arrays.toString(identityMappings.toArray()) + " to map identity: " + authID);
        String mappedIdentity = IdentityMappingUtil.mapIdentity(authID, identityMappings);
        logger.info("Logging out user with mapped identity: " + mappedIdentity);
        jwtServiceUsingTestKeyService.logOut(mappedIdentity);
        logger.info("Logged out user with mapped identity: " + mappedIdentity);
        logger.info("Checking that token for " + mappedIdentity + " is now invalid...");
        jwtServiceUsingTestKeyService.getAuthenticationFromToken(token);

        // Assert
        // Should throw exception when user is not found
    }

    @Test
    public void testShouldLogOutRealmedKerberosUser() throws Exception {
        // Arrange

        expectedException.expect(JwtException.class);
        expectedException.expectMessage("Unable to validate the access token.");

        // Token expires in 60 seconds
        final int EXPIRATION_MILLIS = 60000;
        // map the kerberos identity before we create our token, just as is done in AccessResource
        final String mappedIdentity = IdentityMappingUtil.mapIdentity(REALMED_KERBEROS_IDENTITY, identityMappings);

        LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity,
                EXPIRATION_MILLIS,
                "MockIdentityProvider");
        logger.info("Generating token for " + loginAuthenticationToken);

        // Act
        String token = jwtServiceUsingTestKeyService.generateSignedToken(loginAuthenticationToken);
        logger.info("Generated JWT: " + token);
        logger.info("Validating token...");
        String authID = jwtServiceUsingTestKeyService.getAuthenticationFromToken(token);
        logger.info("Token was valid, unmapped user identity was: " + authID);
        assertEquals(KERBEROS_IDENTITY, authID);
        logger.info("Using identity mappings " + Arrays.toString(identityMappings.toArray()) + " to map identity: " + authID);
        logger.info("Logging out user with mapped identity: " + authID);
        jwtServiceUsingTestKeyService.logOut(authID);
        logger.info("Logged out user with mapped identity: " + authID);
        logger.info("Checking that token for " + authID + " is now invalid...");
        jwtServiceUsingTestKeyService.getAuthenticationFromToken(token);

        // Assert
        // Should throw exception when user is not found
    }

}
