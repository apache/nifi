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
package org.apache.nifi.registry.web.api;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.NiFiRegistryTestApiApplication;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.kerberos.authentication.KerberosTicketValidation;
import org.springframework.security.kerberos.authentication.KerberosTicketValidator;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Deploy the Web API Application using an embedded Jetty Server for local integration testing, with the follow characteristics:
 *
 * - A NiFiRegistryProperties has to be explicitly provided to the ApplicationContext using a profile unique to this test suite.
 * - A NiFiRegistryClientConfig has been configured to create a client capable of completing one-way TLS
 * - The database is embed H2 using volatile (in-memory) persistence
 * - Custom SQL is clearing the DB before each test method by default, unless method overrides this behavior
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.include=ITSecureKerberos")
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = "classpath:db/clearDB.sql")
public class SecureKerberosIT extends IntegrationTestBase {

    private static final String validKerberosTicket = "authenticate_me";
    private static final String invalidKerberosTicket = "do_not_authenticate_me";

    public static class MockKerberosTicketValidator implements KerberosTicketValidator {

        @Override
        public KerberosTicketValidation validateTicket(byte[] token) throws BadCredentialsException {

            boolean validTicket;
            validTicket = Arrays.equals(validKerberosTicket.getBytes(StandardCharsets.UTF_8), token);

            if (!validTicket) {
                throw new BadCredentialsException(MockKerberosTicketValidator.class.getSimpleName() + " does not validate token");
            }

            return new KerberosTicketValidation(
                    "kerberosUser@LOCALHOST",
                    "HTTP/localhsot@LOCALHOST",
                    null,
                    null);
        }
    }

    @Configuration
    @Profile("ITSecureKerberos")
    @Import({NiFiRegistryTestApiApplication.class, SecureITClientConfiguration.class})
    public static class KerberosSpnegoTestConfiguration {

        @Primary
        @Bean
        public static KerberosTicketValidator kerberosTicketValidator() {
            return new MockKerberosTicketValidator();
        }

    }

    private String adminAuthToken;

    @BeforeEach
    public void generateAuthToken() {
        final String validTicket = new String(Base64.getEncoder().encode(validKerberosTicket.getBytes(StandardCharsets.UTF_8)));
        adminAuthToken = client
                .target(createURL("/access/token/kerberos"))
                .request()
                .header("Authorization", "Negotiate " + validTicket)
                .post(null, String.class);
    }

    @Test
    public void testTokenGenerationAndAccessWithNoCredentials() {
        // Given:
        // When: the /access/token/kerberos endpoint is accessed with no credentials
        try (final Response tokenResponse = client
                .target(createURL("/access/token/kerberos"))
                .request()
                .post(null, Response.class)) {

            // Then: the server returns 401 Unauthorized with an authenticate challenge header
            assertEquals(401, tokenResponse.getStatus());
            assertNotNull(tokenResponse.getHeaders().get("www-authenticate"));
            assertEquals(1, tokenResponse.getHeaders().get("www-authenticate").size());
            assertEquals("Negotiate", tokenResponse.getHeaders().get("www-authenticate").get(0));
        }
    }

    @Test
    public void testTokenGenerationAndAccessWithInvalidToken() {
        // Given: invalid kerberos ticket
        final String invalidTicket = new String(Base64.getEncoder().encode(invalidKerberosTicket.getBytes(StandardCharsets.UTF_8)));

        // When: the /access/token/kerberos endpoint is accessed again with an invalid ticket
        try (final Response tokenResponse = client
                .target(createURL("/access/token/kerberos"))
                .request()
                .header("Authorization", "Negotiate " + invalidTicket)
                .post(null, Response.class)) {

            // Then: the server returns 401 Unauthorized
            assertEquals(401, tokenResponse.getStatus());
        }
    }

    @Test
    public void testTokenGenerationAndAccessWithValidToken() throws JSONException {
        final String expectedJwtPayloadJson = "{" +
                "\"sub\":\"kerberosUser@LOCALHOST\"," +
                "\"preferred_username\":\"kerberosUser@LOCALHOST\"," +
                "\"iss\":\"KerberosSpnegoIdentityProvider\"" +
                "}";
        final String expectedAccessStatusJson = "{" +
                "\"identity\":\"kerberosUser@LOCALHOST\"," +
                "\"anonymous\":false}";

        // Given: valid kerberos ticket
        final String validTicket = new String(Base64.getEncoder().encode(validKerberosTicket.getBytes(StandardCharsets.UTF_8)));

        // When: the /access/token/kerberos endpoint is accessed with a valid ticket
        final String token;
        try (final Response tokenResponse = client
                .target(createURL("/access/token/kerberos"))
                .request()
                .header("Authorization", "Negotiate " + validTicket)
                .post(null, Response.class)) {

            // Then: the server returns 200 OK with a JWT in the body
            assertEquals(201, tokenResponse.getStatus());
            token = tokenResponse.readEntity(String.class);
        }
        assertTrue(StringUtils.isNotEmpty(token));
        final String[] jwtParts = token.split("\\.");
        assertEquals(3, jwtParts.length);
        final String jwtPayload = new String(Base64.getDecoder().decode(jwtParts[1]), StandardCharsets.UTF_8);
        JSONAssert.assertEquals(expectedJwtPayloadJson, jwtPayload, false);

        // When: the token is returned in the Authorization header
        final Response accessResponse = client
                .target(createURL("access"))
                .request()
                .header("Authorization", "Bearer " + token)
                .get(Response.class);

        // Then: the server acknowledges the client has access
        assertEquals(200, accessResponse.getStatus());
        final String accessStatus = accessResponse.readEntity(String.class);
        JSONAssert.assertEquals(expectedAccessStatusJson, accessStatus, false);
    }

    @Test
    public void testGetCurrentUser() throws Exception {

        // Given: the client is connected to an unsecured NiFi Registry
        final String expectedJson = "{" +
                "\"identity\":\"kerberosUser@LOCALHOST\"," +
                "\"anonymous\":false," +
                "\"resourcePermissions\":{" +
                "\"anyTopLevelResource\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"buckets\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"tenants\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"policies\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"proxy\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}}" +
                "}";

        // When: the /access endpoint is queried using a JWT for the kerberos user
        final Response response = client
                .target(createURL("/access"))
                .request()
                .header("Authorization", "Bearer " + adminAuthToken)
                .get(Response.class);

        // Then: the server returns a 200 OK with the expected current user
        assertEquals(200, response.getStatus());
        final String actualJson = response.readEntity(String.class);
        JSONAssert.assertEquals(expectedJson, actualJson, false);

    }
}
