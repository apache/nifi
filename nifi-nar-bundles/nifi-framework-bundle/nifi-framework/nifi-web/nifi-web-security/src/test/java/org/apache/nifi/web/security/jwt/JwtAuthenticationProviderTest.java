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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class JwtAuthenticationProviderTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final static int EXPIRATION_MILLIS = 60000;
    private final static String CLIENT_ADDRESS = "127.0.0.1";
    private final static String ADMIN_IDENTITY = "nifiadmin";
    private final static String REALMED_ADMIN_KERBEROS_IDENTITY = "nifiadmin@nifi.apache.org";

    private final static String UNKNOWN_TOKEN = "eyJhbGciOiJIUzI1NiJ9" +
            ".eyJzdWIiOiJ1bmtub3duX3Rva2VuIiwiaXNzIjoiS2VyYmVyb3NQcm9" +
            "2aWRlciIsImF1ZCI6IktlcmJlcm9zUHJvdmlkZXIiLCJwcmVmZXJyZWR" +
            "fdXNlcm5hbWUiOiJ1bmtub3duX3Rva2VuIiwia2lkIjoxLCJleHAiOjE" +
            "2OTI0NTQ2NjcsImlhdCI6MTU5MjQxMTQ2N30.PpOGx3Ul5ydokOOuzKd" +
            "aRKv1kxy6Q4jGy7rBPU8PqxY";

    private NiFiProperties properties;


    private JwtService jwtService;
    private JwtAuthenticationProvider jwtAuthenticationProvider;

    @Before
    public void setUp() throws Exception {
        TestKeyService keyService = new TestKeyService();
        jwtService = new JwtService(keyService);

        // Set up Kerberos identity mappings
        Properties props = new Properties();
        props.put(properties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX, "^(.*?)@(.*?)$");
        props.put(properties.SECURITY_IDENTITY_MAPPING_VALUE_PREFIX, "$1");
        properties = new StandardNiFiProperties(props);

        jwtAuthenticationProvider = new JwtAuthenticationProvider(jwtService, properties, mock(Authorizer.class));
    }

    @Test
    public void testAdminIdentityAndTokenIsValid() throws Exception {
        // Arrange
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(ADMIN_IDENTITY,
                                             EXPIRATION_MILLIS,
                                      "MockIdentityProvider");
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(token, CLIENT_ADDRESS);

        // Act
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();

        // Assert
        assertEquals(ADMIN_IDENTITY, details.getUsername());
    }

    @Test
    public void testKerberosRealmedIdentityAndTokenIsValid() throws Exception {
        // Arrange
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(REALMED_ADMIN_KERBEROS_IDENTITY,
                        EXPIRATION_MILLIS,
                        "MockIdentityProvider");
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(token, CLIENT_ADDRESS);

        // Act
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();

        // Assert
        // Check we now have the mapped identity
        assertEquals(ADMIN_IDENTITY, details.getUsername());
    }

    @Test
    public void testFailToAuthenticateWithUnknownToken() throws Exception {
        // Arrange
        expectedException.expect(InvalidAuthenticationException.class);
        expectedException.expectMessage("Unable to validate the access token.");

        // Generate a token with a known token
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(ADMIN_IDENTITY,
                        EXPIRATION_MILLIS,
                        "MockIdentityProvider");
        jwtService.generateSignedToken(loginAuthenticationToken);

        // Act
        // Try to  authenticate with an unknown token
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(UNKNOWN_TOKEN, CLIENT_ADDRESS);
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);

        // Assert
        // Expect exception
    }

}