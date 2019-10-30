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
package org.apache.nifi.vault;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.vault.service.StandardVaultHttpClientConfiguration;
import org.apache.nifi.vault.service.VaultHttpClientControllerService;
import org.bouncycastle.util.encoders.Hex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultOperations;
import org.springframework.vault.core.VaultSysOperations;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultMount;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Tests VaultHttpClientControllerService class against an HTTP Vault server running in a container.
 *
 * This setup and these tests are reused in the companion test class, {@link VaultHttpsClientControllerServiceTest}.
 */
public class VaultHttpClientControllerServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(VaultHttpClientControllerServiceTest.class);
    private static final SecureRandom random = new SecureRandom();

    static GenericContainer vaultContainer;
    static String vaultToken = getRandomHex(16);
    static String vaultUri;
    static VaultTemplate vaultOperations;

    // This maps KV secrets engine paths to the KV versions under test:
    private final static Map<String, String> kvVersions = new HashMap<>();
    static {
        kvVersions.put("kv1", "1");
        kvVersions.put("kv2", "2");
    }

    // Random strings for the app id and user id values used during app id auth tests:
    private final static String authAppId = getRandomHex(12);
    private final static String authUserId = getRandomHex(6);

    // These are for the app role auth tests, and are initialized when the test container is configured:
    private static String authRoleId;
    private static String authSecretId;

    /**
     * This creates and configures the Vault server test container.
     */
    @BeforeClass
    public static void createTestContainer() {
        int vaultPort = 8200;
        vaultContainer = new GenericContainer<>("vault:latest")
                .withEnv("VAULT_DEV_ROOT_TOKEN_ID", vaultToken)
                .withExposedPorts(vaultPort)
                .waitingFor(Wait.forLogMessage("==> Vault server started.*", 1));

        vaultContainer.start();
        vaultUri = "http://" + vaultContainer.getContainerIpAddress() + ":" + vaultContainer.getMappedPort(vaultPort);
        vaultOperations = new VaultTemplate(VaultEndpoint.from(URI.create(vaultUri)), new TokenAuthentication(vaultToken));
        configureVaultTestEnvironment(vaultOperations);
    }

    /**
     * Given a Vault operations client, this method configures a Vault environment with various kv and auth mounts.
     *
     * @param operations Vault operations client
     */
    static void configureVaultTestEnvironment(VaultOperations operations) {
        VaultSysOperations ops = operations.opsForSys();

        for (String path : kvVersions.keySet()) {
            ops.mount(path, VaultMount.builder().type("kv").options(Collections.singletonMap("version", kvVersions.get(path))).build());
        }

        ops.authMount("app-id", VaultMount.create("app-id"));
        ops.authMount("approle", VaultMount.create("approle"));
        ops.mount("transit", VaultMount.create("transit"));

        // This block creates a vault policy for our apps, users, etc:
        String rules = "{'name': 'test-policy', 'path': {'*': {'policy': 'write'}}}";
        rules = rules.replace("'", "\"");
        operations.write("sys/policy/test-policy", Collections.singletonMap("rules", rules));

        // This block enables our expected "app role" authentication configuration:
        String vaultRoleName = authAppId;
        Map<String, String> config = new HashMap<>();
        config.put("policies", "test-policy");
        config.put("bind_secret_id", "true");
        operations.write("auth/approle/role/" + vaultRoleName, config);
        authRoleId = (String) operations.read(String.format("auth/approle/role/%s/role-id", vaultRoleName)).getData().get("role_id");
        authSecretId = (String) operations.write(String.format("auth/approle/role/%s/secret-id", vaultRoleName), null).getData().get("secret_id");
        logger.info("Constructed 'app role' with role-id=" + authRoleId + " and secret-id=" + authSecretId);

        // This block enables our expected "app id" authentication configuration:
        config = new HashMap<>();
        config.put("value", "test-policy");
        config.put("display_name", authAppId);
        operations.write("auth/app-id/map/app-id/" + authAppId, config);

        config = new HashMap<>();
        config.put("value", authAppId);
        operations.write("auth/app-id/map/user-id/" + authUserId, config);
        logger.info("Constructed 'app id' with app-id:" + authAppId + " user-id:" + authUserId);
    }

    @AfterClass
    public static void stopServerContainer() {
        try {
            vaultContainer.stop();
        } catch (final Exception ignored) {
        } finally {
            try {
                vaultContainer.stop();
            } catch (final Exception ignored) {
            }
        }
        logger.info("Stopped server container.");
    }


    // This test shows we can use token auth when valid.
    @Test
    public void testAuthByToken() throws InitializationException, IOException {
        for (String path : kvVersions.keySet()) {
            VaultHttpClientControllerService service = new VaultHttpClientControllerService();
            TestRunner runner = createTestRunner(StandardVaultHttpClientConfiguration.AUTH_TYPE_TOKEN, vaultUri, path, service);

            // This shows we can't enable the service with an invalid URI
            runner.setProperty(service, VaultHttpClientControllerService.URL, getRandomHex(16));
            runner.setProperty(service, VaultHttpClientControllerService.TOKEN, vaultToken);
            boolean failed = false;
            try {
                runner.enableControllerService(service);
            } catch (final Throwable e) {
                failed = true;
            }
            Assert.assertTrue("Service should not enable with an invalid auth token.", failed);

            // This shows we can't enable the service with an invalid token
            runner.setProperty(service, VaultHttpClientControllerService.URL, vaultUri);
            runner.setProperty(service, VaultHttpClientControllerService.TOKEN, getRandomHex(16));
            failed = false;
            try {
                runner.enableControllerService(service);
            } catch (final Throwable e) {
                failed = true;
            }
            Assert.assertTrue("Service should not enable with an invalid auth token.", failed);

            // Everything valid means everything works:
            runner.setProperty(service, VaultHttpClientControllerService.TOKEN, vaultToken);
            runner.setProperty(service, VaultHttpClientControllerService.URL, vaultUri);
            runKvServiceTests(runner, service);
        }
    }

    // This test shows we can read and write with a valid app id and user id, and cannot with invalid values.
    @Test
    public void testAuthByAppIdAndUserId() throws InitializationException, IOException {
        for (String path : kvVersions.keySet()) {
            VaultHttpClientControllerService service = new VaultHttpClientControllerService();
            TestRunner runner = createTestRunner(StandardVaultHttpClientConfiguration.AUTH_TYPE_APP_ID, vaultUri, path, service);

            // Initialize to all valid values:
            runner.setProperty(service, VaultHttpClientControllerService.APP_ID, authAppId);
            runner.setProperty(service, VaultHttpClientControllerService.USER_ID_MECHANISM, StandardVaultHttpClientConfiguration.APP_ID_STATIC_USER_ID);
            runner.setProperty(service, VaultHttpClientControllerService.USER_ID, authUserId);

            // This shows we can't enable the service with an invalid app id:
            runner.setProperty(service, VaultHttpClientControllerService.APP_ID, getRandomHex(16));
            boolean failed = false;
            try {
                runner.enableControllerService(service);
            } catch (final Throwable e) {
                failed = true;
            }
            Assert.assertTrue("Service should not enable with an invalid app id.", failed);

            // This shows we can't enable the service with an invalid user id:
            runner.setProperty(service, VaultHttpClientControllerService.APP_ID, authAppId);
            runner.setProperty(service, VaultHttpClientControllerService.USER_ID, getRandomHex(16));
            failed = false;
            try {
                runner.enableControllerService(service);
            } catch (final Throwable e) {
                failed = true;
            }
            Assert.assertTrue("Service should not enable with an invalid user id.", failed);

            // Everything valid means everything works:
            runner.setProperty(service, VaultHttpClientControllerService.APP_ID, authAppId);
            runner.setProperty(service, VaultHttpClientControllerService.USER_ID_MECHANISM, StandardVaultHttpClientConfiguration.APP_ID_STATIC_USER_ID);
            runner.setProperty(service, VaultHttpClientControllerService.USER_ID, authUserId);
            runKvServiceTests(runner, service);
        }
    }

    // This test shows we can read and write with a valid role id and user id, and cannot with invalid values.
    @Test
    public void testAuthByAppRole() throws InitializationException, IOException {
        for (String path : kvVersions.keySet()) {
            VaultHttpClientControllerService service = new VaultHttpClientControllerService();
            TestRunner runner = createTestRunner(StandardVaultHttpClientConfiguration.AUTH_TYPE_APP_ROLE, vaultUri, path, service);

            // This shows we can't enable the service with an invalid app id:
            runner.setProperty(service, VaultHttpClientControllerService.ROLE_ID, getRandomHex(16));
            runner.setProperty(service, VaultHttpClientControllerService.SECRET_ID, authSecretId);
            boolean failed = false;
            try {
                runner.enableControllerService(service);
            } catch (final Throwable e) {
                failed = true;
            }
            Assert.assertTrue("Service should not enable with an invalid role id.", failed);

            runner.setProperty(service, VaultHttpClientControllerService.ROLE_ID, authRoleId);
            runner.setProperty(service, VaultHttpClientControllerService.SECRET_ID, getRandomHex(16));
            failed = false;
            try {
                runner.enableControllerService(service);
            } catch (final Throwable e) {
                failed = true;
            }
            Assert.assertTrue("Service should not enable with an invalid user id.", failed);

            // Everything valid means everything works:
            runner.setProperty(service, VaultHttpClientControllerService.ROLE_ID, authRoleId);
            runner.setProperty(service, VaultHttpClientControllerService.SECRET_ID, authSecretId);
            runKvServiceTests(runner, service);
        }
    }

    // This test shows how the service behaves with dynamic properties.
    @Test
    public void testDynamicProperties() throws InitializationException, IOException {
        for (String path : kvVersions.keySet()) {
            VaultHttpClientControllerService service = new VaultHttpClientControllerService();
            TestRunner runner = createTestRunner(StandardVaultHttpClientConfiguration.AUTH_TYPE_TOKEN, vaultUri, path, service);
            runner.setProperty(service, VaultHttpClientControllerService.TOKEN, vaultToken);
            runKvServiceTests(runner, service);

            String propKey = getRandomHex(16);
            Map<String, Object> kvStore = service.list("");
            Assert.assertNotNull(kvStore);
            Assert.assertFalse(kvStore.isEmpty());

            // This shows that a key+value pair visible to the KV store shows up as a property:
            kvStore.put(propKey, getRandomHex(16));
            PropertyDescriptor dynamicProperty = ((ControllerService) service).getPropertyDescriptor(propKey);
            Assert.assertNotNull(dynamicProperty);

            // This shows that another random value can generate a descriptor because the base method constructs one
            // even when our lookup misses:
            dynamicProperty = ((ControllerService) service).getPropertyDescriptor(getRandomHex(16));
            Assert.assertNotNull(dynamicProperty);
        }
    }

    // This method runs a series of read/write tests against the given controller.
    private static void runKvServiceTests(TestRunner runner, VaultHttpClientControllerService controller) throws IOException {
        runner.enableControllerService(controller);
        runner.assertValid(controller);

        Map<String, Object> payload = new HashMap<>(){};
        String head = "key-1-" + getRandomHex(4) + "/" + "key-2-" + getRandomHex(4);
        String tail = "key-3-" + getRandomHex(4);
        String path = head + "/" + tail;
        String root = getRandomHex(4);
        String value = getRandomHex(8);

        payload.put(root, value);
        payload.put(getRandomHex(4), getRandomHex(8));
        payload.put(getRandomHex(4), getRandomHex(8));
        controller.write(path, payload);

        Map<String, Object> response = controller.read(path);
        Assert.assertEquals(payload.keySet().size(), response.keySet().size());
        for (String key : payload.keySet()) {
            Assert.assertEquals(payload.get(key), response.get(key));
        }

        Map<String, Object> kvStore = controller.list(head);
        Assert.assertTrue(kvStore.containsKey(tail));
    }

    TestRunner createTestRunner(String authType, String uri, String path, VaultHttpClientControllerService service) throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("vault-service", service);
        runner.setProperty(service, VaultHttpClientControllerService.URL, uri);
        runner.setProperty(service, VaultHttpClientControllerService.SECRETS_PATH, path);
        runner.setProperty(service, VaultHttpClientControllerService.AUTH_TYPE, authType);
        return runner;
    }

    private static String getRandomHex(int i) {
        byte[] bytes = new byte[i];
        random.nextBytes(bytes);
        return Hex.toHexString(bytes);
    }
}
