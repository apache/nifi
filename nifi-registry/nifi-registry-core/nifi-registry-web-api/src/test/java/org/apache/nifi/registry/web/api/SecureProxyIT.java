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

import org.apache.nifi.registry.NiFiRegistryTestApiApplication;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = NiFiRegistryTestApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.include=ITSecureProxy")
@Import(SecureITClientConfiguration.class)
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql"})
public class SecureProxyIT extends IntegrationTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecureProxyIT.class);

    private static final String INITIAL_ADMIN_IDENTITY = "CN=user1, OU=nifi";
    private static final String PROXY_IDENTITY = "CN=proxy, OU=nifi";
    private static final String NEW_USER_IDENTITY = "CN=user2, OU=nifi";
    private static final String UTF8_USER_IDENTITY = "CN=Алйс, OU=nifi";
    private static final String ANONYMOUS_USER_IDENTITY = "";

    private NiFiRegistryClient registryClient;

    @BeforeEach
    public void setup() {
        final String baseUrl = createBaseURL();
        LOGGER.info("Using base url = " + baseUrl);

        final NiFiRegistryClientConfig clientConfig = createClientConfig(baseUrl);
        assertNotNull(clientConfig);

        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder()
                .config(clientConfig)
                .build();
        assertNotNull(client);
        this.registryClient = client;
    }

    @AfterEach
    public void teardown() {
        try {
            registryClient.close();
        } catch (final Exception e) {
            // do nothing
        }
    }

    @Test
    public void testAccessStatusAsProxiedUser() throws Exception {

        // Given: the client and server have been configured correctly for two-way TLS
        final Permissions noAccess = new Permissions().withCanRead(false).withCanWrite(false).withCanDelete(false);
        final RequestConfig proxiedEntityRequestConfig = new ProxiedEntityRequestConfig(NEW_USER_IDENTITY);

        // When: the /access endpoint is queried using X-ProxiedEntitiesChain
        final UserClient userClient = registryClient.getUserClient(proxiedEntityRequestConfig);
        final CurrentUser currentUser = userClient.getAccessStatus();

        // Then: the server returns the user identity ad
        assertEquals(NEW_USER_IDENTITY, currentUser.getIdentity());
        assertFalse(currentUser.isAnonymous());
        assertNotNull(currentUser.getResourcePermissions());
        assertEquals(noAccess, currentUser.getResourcePermissions().getAnyTopLevelResource());
        assertEquals(noAccess, currentUser.getResourcePermissions().getBuckets());
        assertEquals(noAccess, currentUser.getResourcePermissions().getTenants());
        assertEquals(noAccess, currentUser.getResourcePermissions().getPolicies());
        assertEquals(noAccess, currentUser.getResourcePermissions().getProxy());
    }

    @Test
    public void testAccessStatusAsProxiedAnonymousUser() throws Exception {

        // Given: the client and server have been configured correctly for two-way TLS
        final Permissions noAccess = new Permissions().withCanRead(false).withCanWrite(false).withCanDelete(false);
        final RequestConfig proxiedEntityRequestConfig = new ProxiedEntityRequestConfig(ANONYMOUS_USER_IDENTITY);

        // When: the /access endpoint is queried using X-ProxiedEntitiesChain
        final UserClient userClient = registryClient.getUserClient(proxiedEntityRequestConfig);
        final CurrentUser currentUser = userClient.getAccessStatus();

        // Then: the server returns the proxy identity with default nifi node access
        assertEquals("anonymous", currentUser.getIdentity());
        assertTrue(currentUser.isAnonymous());
        assertNotNull(currentUser.getResourcePermissions());
        assertEquals(noAccess, currentUser.getResourcePermissions().getAnyTopLevelResource());
        assertEquals(noAccess, currentUser.getResourcePermissions().getBuckets());
        assertEquals(noAccess, currentUser.getResourcePermissions().getTenants());
        assertEquals(noAccess, currentUser.getResourcePermissions().getPolicies());
        assertEquals(noAccess, currentUser.getResourcePermissions().getProxy());
    }

    @Test
    public void testAccessStatusAsProxiedUtf8User() throws Exception {

        // Given: the client and server have been configured correctly for two-way TLS
        final Permissions noAccess = new Permissions().withCanRead(false).withCanWrite(false).withCanDelete(false);
        final RequestConfig proxiedEntityRequestConfig = new ProxiedEntityRequestConfig(UTF8_USER_IDENTITY);

        // When: the /access endpoint is queried using X-ProxiedEntitiesChain
        final UserClient userClient = registryClient.getUserClient(proxiedEntityRequestConfig);
        final CurrentUser currentUser = userClient.getAccessStatus();

        // Then: the server returns the proxy identity with default nifi node access
        assertEquals(UTF8_USER_IDENTITY, currentUser.getIdentity());
        assertFalse(currentUser.isAnonymous());
        assertNotNull(currentUser.getResourcePermissions());
        assertEquals(noAccess, currentUser.getResourcePermissions().getAnyTopLevelResource());
        assertEquals(noAccess, currentUser.getResourcePermissions().getBuckets());
        assertEquals(noAccess, currentUser.getResourcePermissions().getTenants());
        assertEquals(noAccess, currentUser.getResourcePermissions().getPolicies());
        assertEquals(noAccess, currentUser.getResourcePermissions().getProxy());
    }

}
