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
package org.apache.nifi.web.security.jwt

import groovy.json.JsonOutput
import net.minidev.json.JSONObject
import org.apache.nifi.web.security.InvalidAuthenticationException
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4.class)
class JwtAuthenticationFilterTest extends GroovyTestCase {

    public static String jwtString

    @Rule
    public ExpectedException expectedException = ExpectedException.none()

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
        final String EXPECTED_PAYLOAD =
                JsonOutput.toJson(
                    sub:'unknownuser',
                    iss:'MockIdentityProvider',
                    aud:'MockIdentityProvider',
                    preferred_username:'unknownuser',
                    kid:1,
                    exp:TOKEN_EXPIRATION_SECONDS,
                    iat:TOKEN_ISSUED_AT)

        // Set up our JWT string with a test token
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
        String authenticationHeader = "Bearer " + jwtString

        // Act
        boolean isValidHeader = new JwtAuthenticationFilter().validJwtFormat(authenticationHeader)

        // Assert
        assertTrue(isValidHeader)
    }

    @Test
    void testMissingBearer() {
        // Arrange

        // Act
        boolean isValidHeader = new JwtAuthenticationFilter().validJwtFormat(jwtString)

        // Assert
        assertFalse(isValidHeader)
    }

    @Test
    void testExtraCharactersAtBeginningOfToken() {
        // Arrange
        String authenticationHeader = "xBearer " + jwtString

        // Act
        boolean isValidToken = new JwtAuthenticationFilter().validJwtFormat(authenticationHeader)

        // Assert
        assertFalse(isValidToken)
    }

    @Test
    void testBadTokenFormat() {
        // Arrange
        String[] tokenStrings = jwtString.split("\\.")
        String badToken = "Bearer " + tokenStrings[1] + tokenStrings[2]

        // Act
        boolean isValidToken = new JwtAuthenticationFilter().validJwtFormat(badToken)

        // Assert
        assertFalse(isValidToken)
    }

    @Test
    void testMultipleTokenInvalid() {
        // Arrange
        String authenticationHeader = "Bearer " + jwtString
        authenticationHeader = authenticationHeader + " " + authenticationHeader

        // Act
        boolean isValidToken = new JwtAuthenticationFilter().validJwtFormat(authenticationHeader)

        // Assert
        assertFalse(isValidToken)
    }

    @Test
    void testExtractToken() {
        // Arrange
        String authenticationHeader = "Bearer " + jwtString

        // Act
        String extractedToken = new JwtAuthenticationFilter().getTokenFromHeader(authenticationHeader)

        // Assert
        assertEquals(jwtString, extractedToken)
    }

    @Test
    void testMultipleTokenDottedInvalid() {
        // Arrange
        String authenticationHeader = "Bearer " + jwtString
        authenticationHeader = authenticationHeader + "." + authenticationHeader

        // Act
        boolean isValidToken = new JwtAuthenticationFilter().validJwtFormat(authenticationHeader)

        // Assert
        assertFalse(isValidToken)
    }

    @Test
    void testMultipleTokenNotExtracted() {
        // Arrange
        expectedException.expect(InvalidAuthenticationException.class)
        expectedException.expectMessage("JWT did not match expected pattern.")
        String authenticationHeader = "Bearer " + jwtString
        authenticationHeader = authenticationHeader + " " + authenticationHeader

        // Act
        String token = new JwtAuthenticationFilter().getTokenFromHeader(authenticationHeader)

        // Assert
        // Expect InvalidAuthenticationException
    }
}
