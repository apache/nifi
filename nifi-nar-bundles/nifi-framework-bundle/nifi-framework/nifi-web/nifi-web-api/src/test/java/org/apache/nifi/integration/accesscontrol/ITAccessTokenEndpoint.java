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
package org.apache.nifi.integration.accesscontrol;

import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;
import javax.ws.rs.core.Response;
import net.minidev.json.JSONObject;
import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.web.api.dto.AccessConfigurationDTO;
import org.apache.nifi.web.api.dto.AccessStatusDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AccessConfigurationEntity;
import org.apache.nifi.web.api.entity.AccessStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.security.jwt.JwtServiceTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Access token endpoint test.
 */
public class ITAccessTokenEndpoint {

    private static OneWaySslAccessControlHelper helper;

    private final String user = "nifiadmin@nifi.apache.org";
    private final String password = "password";
    private static final String CLIENT_ID = "token-endpoint-id";

    @BeforeClass
    public static void setup() throws Exception {
        helper = new OneWaySslAccessControlHelper("src/test/resources/access-control/nifi-mapped-identities.properties");
    }

    // -----------
    // LOGIN CONFIG
    // -----------
    /**
     * Test getting access configuration.
     *
     * @throws Exception ex
     */
    @Test
    public void testGetAccessConfig() throws Exception {
        String url = helper.getBaseUrl() + "/access/config";

        Response response = helper.getUser().testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // extract the process group
        AccessConfigurationEntity accessConfigEntity = response.readEntity(AccessConfigurationEntity.class);

        // ensure there is content
        Assert.assertNotNull(accessConfigEntity);

        // extract the process group dto
        AccessConfigurationDTO accessConfig = accessConfigEntity.getConfig();

        // verify config
        Assert.assertTrue(accessConfig.getSupportsLogin());
    }

    /**
     * Obtains a token and creates a processor using it.
     *
     * @throws Exception ex
     */
    @Test
    public void testCreateProcessorUsingToken() throws Exception {
        String url = helper.getBaseUrl() + "/access/token";

        Response response = helper.getUser().testCreateToken(url, user, password);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the token
        String token = response.readEntity(String.class);

        // attempt to create a processor with it
        createProcessor(token);
    }

    private ProcessorDTO createProcessor(final String token) throws Exception {
        String url = helper.getBaseUrl() + "/process-groups/root/processors";

        // authorization header
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName("Copy");
        processor.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(0l);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        Response response = helper.getUser().testPostWithHeaders(url, entity, headers);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.readEntity(ProcessorEntity.class);

        // verify creation
        processor = entity.getComponent();
        Assert.assertEquals("Copy", processor.getName());
        Assert.assertEquals("org.apache.nifi.integration.util.SourceTestProcessor", processor.getType());

        return processor;
    }

    /**
     * Verifies the response when bad credentials are specified.
     *
     * @throws Exception ex
     */
    @Test
    public void testInvalidCredentials() throws Exception {
        String url = helper.getBaseUrl() + "/access/token";

        Response response = helper.getUser().testCreateToken(url, "user@nifi", "not a real password");

        // ensure the request is not successful
        Assert.assertEquals(400, response.getStatus());
    }

    /**
     * Verifies the response when the user is known.
     *
     * @throws Exception ex
     */
    @Test
    public void testUnknownUser() throws Exception {
        String url = helper.getBaseUrl() + "/access/token";

        Response response = helper.getUser().testCreateToken(url, "not a real user", "not a real password");

        // ensure the request is successful
        Assert.assertEquals(400, response.getStatus());
    }

    /**
     * Request access using access token.
     *
     * @throws Exception ex
     */
    @Test
    public void testRequestAccessUsingToken() throws Exception {
        String accessStatusUrl = helper.getBaseUrl() + "/access";
        String accessTokenUrl = helper.getBaseUrl() + "/access/token";

        Response response = helper.getUser().testGet(accessStatusUrl);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        AccessStatusEntity accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        AccessStatusDTO accessStatus = accessStatusEntity.getAccessStatus();

        // verify unknown
        Assert.assertEquals("UNKNOWN", accessStatus.getStatus());

        response = helper.getUser().testCreateToken(accessTokenUrl, user, password);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the token
        String token = response.readEntity(String.class);

        // authorization header
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);

        // check the status with the token
        response = helper.getUser().testGetWithHeaders(accessStatusUrl, null, headers);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        accessStatus = accessStatusEntity.getAccessStatus();

        // verify unregistered
        Assert.assertEquals("ACTIVE", accessStatus.getStatus());
    }

    // // TODO: Revisit the HTTP status codes in this test after logout functionality change
    // @Ignore("This test is failing before refactoring")
    @Test
    public void testLogOutSuccess() throws Exception {
        String accessStatusUrl = helper.getBaseUrl() + "/access";
        String accessTokenUrl = helper.getBaseUrl() + "/access/token";
        String logoutUrl = helper.getBaseUrl() + "/access/logout";

        Response response = helper.getUser().testGet(accessStatusUrl);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        AccessStatusEntity accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        AccessStatusDTO accessStatus = accessStatusEntity.getAccessStatus();

        // verify unknown
        Assert.assertEquals("UNKNOWN", accessStatus.getStatus());

        response = helper.getUser().testCreateToken(accessTokenUrl, user, password);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the token
        String token = response.readEntity(String.class);

        // authorization header
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);

        // check the status with the token
        response = helper.getUser().testGetWithHeaders(accessStatusUrl, null, headers);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        accessStatus = accessStatusEntity.getAccessStatus();

        // verify unregistered
        Assert.assertEquals("ACTIVE", accessStatus.getStatus());

        // log out
        response = helper.getUser().testDeleteWithHeaders(logoutUrl, headers);
        Assert.assertEquals(200, response.getStatus());

        // ensure we can no longer use our token
        response = helper.getUser().testGetWithHeaders(accessStatusUrl, null, headers);
        Assert.assertEquals(401, response.getStatus());
    }

    @Test
    public void testLogOutNoTokenHeader() throws Exception {
        String accessStatusUrl = helper.getBaseUrl() + "/access";
        String accessTokenUrl = helper.getBaseUrl() + "/access/token";
        String logoutUrl = helper.getBaseUrl() + "/access/logout";

        Response response = helper.getUser().testGet(accessStatusUrl);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        AccessStatusEntity accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        AccessStatusDTO accessStatus = accessStatusEntity.getAccessStatus();

        // verify unknown
        Assert.assertEquals("UNKNOWN", accessStatus.getStatus());

        response = helper.getUser().testCreateToken(accessTokenUrl, user, password);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the token
        String token = response.readEntity(String.class);

        // authorization header
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);

        // check the status with the token
        response = helper.getUser().testGetWithHeaders(accessStatusUrl, null, headers);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        accessStatus = accessStatusEntity.getAccessStatus();

        // verify unregistered
        Assert.assertEquals("ACTIVE", accessStatus.getStatus());


        // log out should fail as we provided no token for logout to use
        response = helper.getUser().testDeleteWithHeaders(logoutUrl, null);
        Assert.assertEquals(401, response.getStatus());
    }

    @Test
    public void testLogOutUnknownToken() throws Exception {
        // Arrange
        final String ALG_HEADER = "{\"alg\":\"HS256\"}";
        final int EXPIRATION_SECONDS = 60;
        Calendar now = Calendar.getInstance();
        final long currentTime = (long) (now.getTimeInMillis() / 1000.0);
        final long TOKEN_ISSUED_AT = currentTime;
        final long TOKEN_EXPIRATION_SECONDS = currentTime + EXPIRATION_SECONDS;

        // Always use LinkedHashMap to enforce order of the keys because the signature depends on order
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("sub", "unknownuser");
        claims.put("iss", "MockIdentityProvider");
        claims.put("aud", "MockIdentityProvider");
        claims.put("preferred_username", "unknownuser");
        claims.put("kid", 1);
        claims.put("exp", TOKEN_EXPIRATION_SECONDS);
        claims.put("iat", TOKEN_ISSUED_AT);
        final String EXPECTED_PAYLOAD = new JSONObject(claims).toString();

        String accessStatusUrl = helper.getBaseUrl() + "/access";
        String accessTokenUrl = helper.getBaseUrl() + "/access/token";
        String logoutUrl = helper.getBaseUrl() + "/access/logout";

        Response response = helper.getUser().testCreateToken(accessTokenUrl, user, password);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());
        // get the token
        String token = response.readEntity(String.class);
        // authorization header
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);
        // check the status with the token
        response = helper.getUser().testGetWithHeaders(accessStatusUrl, null, headers);
        Assert.assertEquals(200, response.getStatus());

        // Generate a token that will not match signatures with the generated token.
        final String UNKNOWN_USER_TOKEN = JwtServiceTest.generateHS256Token(ALG_HEADER, EXPECTED_PAYLOAD, true, true);
        Map<String, String> badHeaders = new HashMap<>();
        badHeaders.put("Authorization", "Bearer " + UNKNOWN_USER_TOKEN);

        // Log out should fail as we provide a bad token to use, signatures will mismatch
        response = helper.getUser().testGetWithHeaders(logoutUrl, null, badHeaders);
        Assert.assertEquals(401, response.getStatus());
    }

    @Test
    public void testLogOutSplicedTokenSignature() throws Exception {
        // Arrange
        final String ALG_HEADER = "{\"alg\":\"HS256\"}";
        final int EXPIRATION_SECONDS = 60;
        Calendar now = Calendar.getInstance();
        final long currentTime = (long) (now.getTimeInMillis() / 1000.0);
        final long TOKEN_ISSUED_AT = currentTime;
        final long TOKEN_EXPIRATION_SECONDS = currentTime + EXPIRATION_SECONDS;

        String accessTokenUrl = helper.getBaseUrl() + "/access/token";
        String logoutUrl = helper.getBaseUrl() + "/access/logout";

        Response response = helper.getUser().testCreateToken(accessTokenUrl, user, password);
        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());
        // replace the user in the token with an unknown user
        String realToken = response.readEntity(String.class);
        String realSignature = realToken.split("\\.")[2];

        // Generate a token that we will add a valid signature from a different token
        // Always use LinkedHashMap to enforce order of the keys because the signature depends on order
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("sub", "unknownuser");
        claims.put("iss", "MockIdentityProvider");
        claims.put("aud", "MockIdentityProvider");
        claims.put("preferred_username", "unknownuser");
        claims.put("kid", 1);
        claims.put("exp", TOKEN_EXPIRATION_SECONDS);
        claims.put("iat", TOKEN_ISSUED_AT);
        final String EXPECTED_PAYLOAD = new JSONObject(claims).toString();
        final String tempToken = JwtServiceTest.generateHS256Token(ALG_HEADER, EXPECTED_PAYLOAD, true, true);

        // Splice this token with the real token from above
        String[] splitToken = tempToken.split("\\.");
        StringJoiner joiner = new StringJoiner(".");
        joiner.add(splitToken[0]);
        joiner.add(splitToken[1]);
        joiner.add(realSignature);
        String splicedUserToken = joiner.toString();

        Map<String, String> badHeaders = new HashMap<>();
        badHeaders.put("Authorization", "Bearer " + splicedUserToken);

        // Log out should fail as we provide a bad token to use, signatures will mismatch
        response = helper.getUser().testGetWithHeaders(logoutUrl, null, badHeaders);
        Assert.assertEquals(401, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
