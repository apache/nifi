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

import jakarta.ws.rs.ForbiddenException;
import org.apache.nifi.registry.NiFiRegistryTestApiApplication;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
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

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = NiFiRegistryTestApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.include=ITSecureFile")
@Import(SecureITClientConfiguration.class)
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql", "classpath:db/FlowsIT.sql"})
public class SecureNiFiRegistryClientIT extends IntegrationTestBase {

    static final Logger LOGGER = LoggerFactory.getLogger(SecureNiFiRegistryClientIT.class);

    static final String INITIAL_ADMIN_IDENTITY = "CN=user1, OU=nifi";
    static final String NO_ACCESS_IDENTITY = "CN=no-access, OU=nifi";

    static final String SECOND_IDENTITY = "CN=keep_author, OU=nifi";

    private NiFiRegistryClient client;

    @BeforeEach
    public void setup() {
        final String baseUrl = createBaseURL();
        LOGGER.info("Using base url = {}", baseUrl);

        final NiFiRegistryClientConfig clientConfig = createClientConfig(baseUrl);
        assertNotNull(clientConfig);

        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder()
                .config(clientConfig)
                .build();
        assertNotNull(client);
        this.client = client;
    }

    @AfterEach
    public void teardown() {
        try {
            client.close();
        } catch (Exception ignored) {

        }
    }

    @Test
    public void testGetAccessStatusWithProxiedEntity() throws IOException, NiFiRegistryException {
        final String proxiedEntity = "user2";
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        final UserClient userClient = client.getUserClient(requestConfig);
        final CurrentUser status = userClient.getAccessStatus();
        assertEquals("user2", status.getIdentity());
        assertFalse(status.isAnonymous());
    }

    @Test
    public void testCreatedBucketWithProxiedEntity() throws IOException, NiFiRegistryException {
        final String proxiedEntity = "user2";
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        final BucketClient bucketClient = client.getBucketClient(requestConfig);

        final Bucket bucket = new Bucket();
        bucket.setName("Bucket 1");
        bucket.setDescription("This is bucket 1");
        bucket.setRevision(new RevisionInfo(null, 0L));

        try {
            bucketClient.create(bucket);
            fail("Shouldn't have been able to create a bucket");
        } catch (Exception ignored) {

        }
    }

    @Test
    public void testDirectFlowAccess() throws IOException {
        // this user shouldn't have access to anything
        final String proxiedEntity = NO_ACCESS_IDENTITY;

        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        final FlowClient proxiedFlowClient = client.getFlowClient(requestConfig);
        final FlowSnapshotClient proxiedFlowSnapshotClient = client.getFlowSnapshotClient(requestConfig);

        try {
            proxiedFlowClient.get("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertInstanceOf(ForbiddenException.class, e.getCause());
        }

        try {
            proxiedFlowSnapshotClient.getLatest("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertInstanceOf(ForbiddenException.class, e.getCause());
        }

        try {
            proxiedFlowSnapshotClient.getLatestMetadata("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertInstanceOf(ForbiddenException.class, e.getCause());
        }

        try {
            proxiedFlowSnapshotClient.get("1", 1);
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertInstanceOf(ForbiddenException.class, e.getCause());
        }

        try {
            proxiedFlowSnapshotClient.getSnapshotMetadata("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertInstanceOf(ForbiddenException.class, e.getCause());
        }

    }

}
