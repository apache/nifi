package org.apache.nifi.web.security.jwt;

import groovy.json.JsonOutput;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import javax.servlet.http.HttpServletRequest;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NiFiBearerTokenResolverTest {

    public static String jwtString;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private static HttpServletRequest request;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        final String ALG_HEADER = "{\"alg\":\"HS256\"}";
        final int EXPIRATION_SECONDS = 500;
        Calendar now = Calendar.getInstance();
        final long currentTime = (long) (now.getTimeInMillis() / 1000.0);
        final long TOKEN_ISSUED_AT = currentTime;
        final long TOKEN_EXPIRATION_SECONDS = currentTime + EXPIRATION_SECONDS;

        Map<String, String> hashMap = new HashMap<String, String>()
        {{
            put("sub", "unknownuser");
            put("iss", "MockIdentityProvider");
            put("aud", "MockIdentityProvider");
            put("preferred_username", "unknownuser");
            put("kid", String.valueOf(1));
            put("exp", String.valueOf(TOKEN_EXPIRATION_SECONDS));
            put("iat", String.valueOf(TOKEN_ISSUED_AT));
        }};

        // Generate a token that we will add a valid signature from a different token
        // Always use LinkedHashMap to enforce order of the keys because the signature depends on order
        final String EXPECTED_PAYLOAD = JsonOutput.toJson(hashMap);

        // Set up our JWT string with a test token
        jwtString = JwtServiceTest.generateHS256Token(ALG_HEADER, EXPECTED_PAYLOAD, true, true);

        request = mock(HttpServletRequest.class);
    }

    @Test
    public void testValidAuthenticationHeaderString() {
        // Arrange
        String authenticationHeader = "Bearer " + jwtString;
        when(request.getHeader(eq(NiFiBearerTokenResolver.AUTHORIZATION))).thenReturn(authenticationHeader);

        // Act
        String isValidHeader = new NiFiBearerTokenResolver().resolve(request);

        // Assert
        assertEquals(jwtString, isValidHeader);
    }

    @Test
    public void testMissingBearer() {
        // Arrange
        String authenticationHeader = jwtString;
        when(request.getHeader(eq(NiFiBearerTokenResolver.AUTHORIZATION))).thenReturn(authenticationHeader);

        // Act
        String resolvedToken = new NiFiBearerTokenResolver().resolve(request);

        // Assert
        assertNull(resolvedToken);
    }

    @Test
    public void testExtraCharactersAtBeginningOfToken() {
        // Arrange
        String authenticationHeader = "xBearer " + jwtString;
        when(request.getHeader(eq(NiFiBearerTokenResolver.AUTHORIZATION))).thenReturn(authenticationHeader);

        // Act
        String resolvedToken = new NiFiBearerTokenResolver().resolve(request);

        // Assert
        assertNull(resolvedToken);
    }

    @Test
    public void testBadTokenFormat() {
        // Arrange
        String[] tokenStrings = jwtString.split("\\.");
        when(request.getHeader(eq(NiFiBearerTokenResolver.AUTHORIZATION))).thenReturn(String.valueOf("Bearer " + tokenStrings[1] + tokenStrings[2]));

        // Act
        String resolvedToken = new NiFiBearerTokenResolver().resolve(request);

        // Assert
        assertNull(resolvedToken);
    }

    @Test
    public void testMultipleTokenInvalid() {
        // Arrange
        String authenticationHeader = "Bearer " + jwtString;
        when(request.getHeader(eq(NiFiBearerTokenResolver.AUTHORIZATION))).thenReturn(String.format("%s %s", authenticationHeader, authenticationHeader));

        // Act
        String resolvedToken = new NiFiBearerTokenResolver().resolve(request);

        // Assert
        assertNull(resolvedToken);
    }

    @Test
    public void testExtractToken() {
        // Arrange
        String authenticationHeader = "Bearer " + jwtString;
        when(request.getHeader(eq(NiFiBearerTokenResolver.AUTHORIZATION))).thenReturn(authenticationHeader);

        // Act
        String extractedToken = new NiFiBearerTokenResolver().resolve(request);

        // Assert
        assertEquals(jwtString, extractedToken);
    }
}