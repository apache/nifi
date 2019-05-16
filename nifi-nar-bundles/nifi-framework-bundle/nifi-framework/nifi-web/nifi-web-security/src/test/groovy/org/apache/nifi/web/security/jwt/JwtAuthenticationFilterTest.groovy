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

import net.minidev.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass
import org.junit.Rule;
import org.junit.Test
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
class JwtAuthenticationFilterTest extends GroovyTestCase {

    public static String jwtString;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUpOnce() throws Exception {
        final String ALG_HEADER = "{\"alg\":\"HS256\"}"
        final int EXPIRATION_SECONDS = 500
        Calendar now = Calendar.getInstance()
        final long currentTime = (long) (now.getTimeInMillis() / 1000.0)
        final long TOKEN_ISSUED_AT = currentTime
        final long TOKEN_EXPIRATION_SECONDS = currentTime + EXPIRATION_SECONDS

        // Generate a token that we will add a valid signature from a different token
        // Always use LinkedHashMap to enforce order of the keys because the signature depends on order
        Map<String, Object> claims = new LinkedHashMap<>()
        claims.put("sub", "unknownuser")
        claims.put("iss", "MockIdentityProvider")
        claims.put("aud", "MockIdentityProvider")
        claims.put("preferred_username", "unknownuser")
        claims.put("kid", 1)
        claims.put("exp", TOKEN_EXPIRATION_SECONDS)
        claims.put("iat", TOKEN_ISSUED_AT)
        final String EXPECTED_PAYLOAD = new JSONObject(claims).toString()
        jwtString = JwtServiceTest.generateHS256Token(ALG_HEADER, EXPECTED_PAYLOAD, true, true)

    }

    @AfterClass
    public static void tearDownOnce() throws Exception {

    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    void testValidAuthenticationHeaderString() {
        // Arrange
        String authenticationHeader = ("Bearer " + jwtString)

        // Act
        boolean validHeader = new JwtAuthenticationFilter().validJwtFormat(authenticationHeader)

        // Assert
        assertTrue(validHeader)
    }

    @Test
    void testMissingBearer() {
        // Arrange

        // Act
        boolean invalidHeader = new JwtAuthenticationFilter().validJwtFormat(jwtString)

        // Assert
        assertFalse(invalidHeader)
    }

    @Test
    void testBadTokenFormat() {
        // Arrange
        String[] tokenStrings = jwtString.split("\\.")
        String badToken = "Bearer " + tokenStrings[1] + tokenStrings[2]

        // Act
        boolean invalidToken = new JwtAuthenticationFilter().validJwtFormat(badToken)

        // Assert
        assertFalse(invalidToken)
    }

    @Test
    void testExtractToken() {
        // Arrange
        String authenticationHeader = ("Bearer " + jwtString)

        // Act
        String validToken = new JwtAuthenticationFilter().getTokenFromHeader(authenticationHeader)

        // Assert
        assertEquals(jwtString, validToken)
    }


    @Test
    void testMultipleTokenInvalid() {
        // Arrange
        String authenticationHeader = ("Bearer " + jwtString)
        authenticationHeader = authenticationHeader + " " + authenticationHeader

        // Act
        boolean validToken = new JwtAuthenticationFilter().validJwtFormat(authenticationHeader)

        // Assert
        assertFalse(validToken)
    }

    @Test
    void testMultipleTokenDottedInvalid() {
        // Arrange
        String authenticationHeader = ("Bearer " + jwtString)
        authenticationHeader = authenticationHeader + "." + authenticationHeader

        // Act
        boolean validToken = new JwtAuthenticationFilter().validJwtFormat(authenticationHeader)

        // Assert
        assertFalse(validToken)
    }

    @Test
    void testMultipleTokenNotExtracted() {
        // Arrange
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("No match found");
        String authenticationHeader = ("Bearer " + jwtString)
        authenticationHeader = authenticationHeader + " " + authenticationHeader

        // Act
        String token = new JwtAuthenticationFilter().getTokenFromHeader(authenticationHeader)

        // Assert
        // Expect exception
    }
}
