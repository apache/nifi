package org.apache.nifi.web.security.jwt;

import io.jsonwebtoken.JwtException;
import org.apache.commons.codec.CharEncoding;
import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.KeyService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by alopresto on 11/11/15.
 */
public class JwtServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(JwtServiceTest.class);

    /**
     * These constant strings were generated using the tool at http://jwt.io
     */

    private static final String VALID_SIGNED_TOKEN = "";
    private static final String INVALID_SIGNED_TOKEN = "";
    private static final String VALID_UNSIGNED_TOKEN = "";
    private static final String INVALID_UNSIGNED_TOKEN = "";
    private static final String VALID_MALSIGNED_TOKEN = "";
    private static final String INVALID_MALSIGNED_TOKEN = "";

    private static final String DEFAULT_HEADER = "{\"alg\":\"HS256\"}";

    private static final String TOKEN_DELIMITER = ".";

    private static final String HMAC_SECRET = "test_hmac_shared_secret";

    private KeyService mockKeyService;

    // Class under test
    private JwtService jwtService;


    private String generateHS256Token(String rawHeader, String rawPayload, boolean isValid, boolean isSigned) {
        return generateHS256Token(rawHeader, rawPayload, HMAC_SECRET, isValid, isSigned);
    }

    private String generateHS256Token(String rawHeader, String rawPayload, String hmacSecret, boolean isValid, boolean isSigned) {
        try {
            logger.info("Generating token for " + rawHeader + " + " + rawPayload);

            String base64Header = Base64.encodeBase64URLSafeString(rawHeader.getBytes(CharEncoding.UTF_8));
            String base64Payload = Base64.encodeBase64URLSafeString(rawPayload.getBytes(CharEncoding.UTF_8));
            // TODO: Support valid/invalid manipulation

            final String body = base64Header + TOKEN_DELIMITER + base64Payload;

            String signature = generateHMAC(hmacSecret, body);

            return body + TOKEN_DELIMITER + signature;
        } catch (NoSuchAlgorithmException | InvalidKeyException | UnsupportedEncodingException e) {
            final String errorMessage = "Could not generate the token";
            logger.error(errorMessage, e);
            fail(errorMessage);
            return null;
        }
    }

    private String generateHMAC(String hmacSecret, String body) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeyException {
        Mac hmacSHA256 = Mac.getInstance("HmacSHA256");
        SecretKeySpec secret_key = new SecretKeySpec(hmacSecret.getBytes("UTF-8"), "HmacSHA256");
        hmacSHA256.init(secret_key);
        return Base64.encodeBase64URLSafeString(hmacSHA256.doFinal(body.getBytes("UTF-8")));
    }


    @Before
    public void setUp() throws Exception {
        mockKeyService = Mockito.mock(KeyService.class);
        when(mockKeyService.getOrCreateKey(anyString())).thenReturn(HMAC_SECRET);
        jwtService = new JwtService(mockKeyService);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testShouldGetAuthenticationForValidToken() throws Exception {

    }

    @Test
    public void testShouldNotGetAuthenticationForInvalidToken() throws Exception {
        // Arrange
        String token = INVALID_SIGNED_TOKEN;

        String header = "{" +
                "  \"alg\":\"HS256\"" +
                "}";
        String payload = "{" +
                "  \"sub\":\"alopresto\"," +
                "  \"preferred_username\":\"alopresto\"," +
                "  \"exp\":2895419760" +
                "}";

        // Act
        logger.info("Test token: " + generateHS256Token(header, payload, true, true));


        // Assert


    }

    @Test
    public void testShouldNotGetAuthenticationForEmptyToken() throws Exception {

    }

    @Test
    public void testShouldNotGetAuthenticationForUnsignedToken() throws Exception {

    }

    @Test
    public void testShouldNotGetAuthenticationForTokenWithWrongAlgorithm() throws Exception {

    }

    @Test
    public void testShouldGenerateSignedToken() throws Exception {
        // Arrange

        // Token expires in 60 seconds
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken("alopresto", EXPIRATION_MILLIS, "MockIdentityProvider");
        logger.debug("Generating token for " + loginAuthenticationToken);

        final String EXPECTED_HEADER = DEFAULT_HEADER;

        // Convert the expiration time from ms to s
        final long TOKEN_EXPIRATION_SEC = (long) (loginAuthenticationToken.getExpiration() / 1000.0);

        // Act
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        logger.debug("Generated JWT: " + token);

        // Run after the SUT generates the token to ensure the same issued at time

        // Split the token, decode the middle section, and form a new String
        final String DECODED_PAYLOAD = new String(Base64.decodeBase64(token.split("\\.")[1].getBytes()));
        final long ISSUED_AT_SEC = Long.valueOf(DECODED_PAYLOAD.substring(DECODED_PAYLOAD.lastIndexOf(":") + 1, DECODED_PAYLOAD.length() - 1));
        logger.trace("Actual token was issued at " + ISSUED_AT_SEC);

        // Always use LinkedHashMap to enforce order of the keys because the signature depends on order
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("sub", "alopresto");
        claims.put("iss", "MockIdentityProvider");
        claims.put("aud", "MockIdentityProvider");
        claims.put("preferred_username", "alopresto");
        claims.put("exp", TOKEN_EXPIRATION_SEC);
        claims.put("iat", ISSUED_AT_SEC);
        logger.trace("JSON Object to String: " + new JSONObject(claims).toString());

        final String EXPECTED_PAYLOAD = new JSONObject(claims).toString();
        final String EXPECTED_TOKEN_STRING = generateHS256Token(EXPECTED_HEADER, EXPECTED_PAYLOAD, true, true);
        logger.debug("Expected JWT: " + EXPECTED_TOKEN_STRING);

        // Assert
        assertEquals("JWT token", EXPECTED_TOKEN_STRING, token);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShouldNotGenerateTokenWithNullAuthenticationToken() throws Exception {
        // Arrange
        LoginAuthenticationToken nullLoginAuthenticationToken = null;
        logger.debug("Generating token for " + nullLoginAuthenticationToken);

        // Act
        jwtService.generateSignedToken(nullLoginAuthenticationToken);

        // Assert

        // Should throw exception
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGenerateTokenWithEmptyIdentity() throws Exception {
        // Arrange
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken emptyIdentityLoginAuthenticationToken = new LoginAuthenticationToken("", EXPIRATION_MILLIS, "MockIdentityProvider");
        logger.debug("Generating token for " + emptyIdentityLoginAuthenticationToken);

        // Act
        jwtService.generateSignedToken(emptyIdentityLoginAuthenticationToken);

        // Assert

        // Should throw exception
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGenerateTokenWithNullIdentity() throws Exception {
        // Arrange
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken nullIdentityLoginAuthenticationToken = new LoginAuthenticationToken(null, EXPIRATION_MILLIS, "MockIdentityProvider");
        logger.debug("Generating token for " + nullIdentityLoginAuthenticationToken);

        // Act
        jwtService.generateSignedToken(nullIdentityLoginAuthenticationToken);

        // Assert

        // Should throw exception
    }

    @Test(expected = JwtException.class)
    public void testShouldNotGenerateTokenWithMissingKey() throws Exception {
        // Arrange
        final int EXPIRATION_MILLIS = 60000;
        LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken("alopresto", EXPIRATION_MILLIS, "MockIdentityProvider");
        logger.debug("Generating token for " + loginAuthenticationToken);

        // Set up the bad key service
        KeyService missingKeyService = Mockito.mock(KeyService.class);
        when(missingKeyService.getOrCreateKey(anyString())).thenThrow(new AdministrationException("Could not find a key for that user"));
        jwtService = new JwtService(missingKeyService);

        // Act
        jwtService.generateSignedToken(loginAuthenticationToken);

        // Assert

        // Should throw exception
    }
}