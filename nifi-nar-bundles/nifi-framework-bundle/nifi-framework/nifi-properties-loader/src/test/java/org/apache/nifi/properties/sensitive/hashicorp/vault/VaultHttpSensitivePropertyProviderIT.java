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
package org.apache.nifi.properties.sensitive.hashicorp.vault;

import org.apache.nifi.properties.sensitive.AbstractSensitivePropertyProviderTest;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.CipherUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.vault.VaultException;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultOperations;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultMount;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.URI;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * These tests use a containerized Vault server in "dev" mode to test our Vault Sensitive Property Provider.
 * The image starts a Vault server with an HTTP endpoint, and uses the random token we pass into it as
 * its initial root token.
 *
 * The tests manipulate the system properties, much in the same way that a user would set Vault values
 * by way of setting system properties (or environment variables).
 *
 * Before the tests are run, we have a static method to configure the Vault server to match our expectations,
 * also much in the same way that a user would set Vault options by way of the Vault CLI or API.
 *
 * These tests are completely self-contained by way of using Docker containers.  No user keys are ever referenced,
 * and the entire test environment, including the Vault server itself, is constructed from scratch before each test
 * session.
 */
public class VaultHttpSensitivePropertyProviderIT<x> extends AbstractSensitivePropertyProviderTest  {
    private static final Logger logger = LoggerFactory.getLogger(VaultHttpSensitivePropertyProviderIT.class);
    private static final SecureRandom random = new SecureRandom();

    static GenericContainer vaultContainer;
    static VaultOperations vaultOperations;

    static final String vaultToken = CipherUtils.getRandomHex(16);
    private static final int vaultPort = 8200;
    static String vaultUri;

    // For app role auth tests:
    private static String authRoleId;
    private static String authSecretId;

    // For app id auth tests:
    private static String authAppId;
    private static String authUserId;

    // For token and cubbyhole auth types:
    private static String transitKeyId;
    private static String cubbyholeTokenId;

    // System property names that we clear and check before each test:
    private Set<String> vaultSystemPropertyNames = Stream.of(
            "vault.ssl.trust-store",
            "vault.ssl.key-store",
            "vault.authentication",
            "vault.uri",
            "vault.token",
            "vault.app-role.role-id",
            "vault.app-role.secret-id",
            "vault.app-id.app-id",
            "vault.app-id.user-id"
    ).collect(Collectors.toSet());

    /**
     * This method creates the Vault environment needed by the test cases.
     */
    @BeforeClass
    public static void createTestContainer() {
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
     * Given a Vault operations client, this method configures a Vault environment for our unit tests.
     *
     * @param operations Vault operations client
     */
    static void configureVaultTestEnvironment(VaultOperations operations) {
        try {
            operations.opsForSys().authMount("app-id", VaultMount.create("app-id"));
            operations.opsForSys().authMount("approle", VaultMount.create("approle"));
            operations.opsForSys().mount("transit", VaultMount.create("transit"));
        } catch (final VaultException ignored) {
        }

        // This block creates a vault policy for our apps, users, etc:
        String rules = "{'name': 'test-policy', 'path': {'*': {'policy': 'write'}}}";
        rules = rules.replace("'", "\"");
        operations.write("sys/policy/test-policy", Collections.singletonMap("rules", rules));

        // This block enables our expected "app role" authentication configuration:
        authAppId = CipherUtils.getRandomHex(12);
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
        authUserId = CipherUtils.getRandomHex(6);
        config = new HashMap<>();
        config.put("value", authAppId);
        operations.write("auth/app-id/map/user-id/" + authUserId, config);
        logger.info("Constructed 'app id' with app-id:" + authAppId + " user-id:" + authUserId);

        // This block creates a transit key, much like a user would create via the vault ui or cli:
        transitKeyId = CipherUtils.getRandomHex(12);
        operations.opsForTransit().createKey(transitKeyId);
        logger.info("Constructed 'transit key' with key-id:" + transitKeyId);

        // This block creates a token for cubbyhole authentication:
        cubbyholeTokenId = operations.opsForToken().create().getToken().getToken();
        config = new HashMap<>();
        config.put("token", cubbyholeTokenId);
        operations.write("cubbyhole/token", config);
        logger.info("Constructed 'cubbyhole token' with token-id:" + cubbyholeTokenId);
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

    /**
     * This method ensures we start each test without any "vault.*" properties set.
     */
    @Before
    public void checkSystemProperties() {
        for (String name : vaultSystemPropertyNames) {
            Assert.assertNull(System.getProperty(name));
        }
    }

    /**
     * This ensures we clear the system properties after each test.
     */
    @After
    public void clearSystemProperties() {
        for (String name : vaultSystemPropertyNames) {
            System.clearProperty(name);
        }
    }

    /**
     * This test shows that the Vault operations client client works as expected:  we can call the transit endpoint to encrypt/decrypt.
     */
    @Test
    public void testBaselineClientShouldWrapAndUnwrapDirectly() throws Exception {
        int dataSize = CipherUtils.getRandomInt(12, 64);
        String data = CipherUtils.getRandomHex(dataSize);

        String wrapped = VaultSensitivePropertyProvider.vaultTransitEncrypt(vaultOperations, transitKeyId, data);
        String unwrapped = VaultSensitivePropertyProvider.vaultTransitDecrypt(vaultOperations, transitKeyId, wrapped);
        Assert.assertEquals(unwrapped, data);

        logger.info("Wrapped, unwrapped, and matched map of " + dataSize + " random bytes during initial checks.");
    }

    /**
     * This test shows that we can specify token authentication and that type gives a provider that encrypts and decrypts text as expected.
     */
    @Test
    public void testProviderShouldProtectAndUnprotectWithTokenAuthType() throws Exception {
        setTokenAuthProps();

        String keyId = VaultSensitivePropertyProvider.formatForTokenAuth(transitKeyId);
        VaultSensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
        int plainSize = CipherUtils.getRandomInt(32, 256);

        checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
        logger.info("Token auth protected and unprotected string of " + plainSize + " bytes using material: " + keyId);
    }

    /**
     * This test shows that we can specify app role authentication and that type gives a provider that encrypts and decrypts text as expected.
     */
    @Test
    public void testProviderShouldProtectAndUnprotectWithAppRoleAuthType() throws Exception {
        System.setProperty("vault.authentication", VaultSensitivePropertyProvider.VAULT_AUTH_APP_ROLE);
        System.setProperty("vault.uri", vaultUri);
        System.setProperty("vault.app-role.role-id", authRoleId);
        System.setProperty("vault.app-role.secret-id", authSecretId);

        String keyId = VaultSensitivePropertyProvider.formatForAppRoleAuth(transitKeyId);
        VaultSensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
        int plainSize = CipherUtils.getRandomInt(32, 256);

        checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
        logger.info("App Role auth protected and unprotected string of " + plainSize + " bytes using material: " + keyId);
    }

    /**
     * This test shows that we can specify app role authentication and that type gives a provider that encrypts and decrypts text as expected.
     */
    @Test
    public void testProviderShouldProtectAndUnprotectWithAppIdAuthType() throws Exception {
        System.setProperty("vault.authentication", VaultSensitivePropertyProvider.VAULT_AUTH_APP_ID);
        System.setProperty("vault.uri", vaultUri);
        System.setProperty("vault.app-id.app-id", authAppId);
        System.setProperty("vault.app-id.user-id", authUserId);

        String keyId = VaultSensitivePropertyProvider.formatForAppIdAuth(transitKeyId);
        VaultSensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
        int plainSize = CipherUtils.getRandomInt(32, 256);

        checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
        logger.info("App Id auth protected and unprotected string of " + plainSize + " bytes using material: " + keyId);
    }

    /**
     * This test shows that we can specify token authentication and that type gives a provider that wraps and unwraps text as expected.
     */
    @Test
    public void testProviderShouldProtectAndUnprotectWithCubbyholeAuthType() throws Exception {
        System.setProperty("vault.authentication", VaultSensitivePropertyProvider.VAULT_AUTH_CUBBYHOLE);
        System.setProperty("vault.uri", vaultUri);
        System.setProperty("vault.token", vaultToken);

        String keyId = VaultSensitivePropertyProvider.formatForCubbyholeAuth(cubbyholeTokenId);
        VaultSensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
        int plainSize = CipherUtils.getRandomInt(32, 256);

        checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
        logger.info("Cubbyhole auth protected and unprotected string of " + plainSize + " bytes using material: " + keyId);
    }

    /**
     * This test shows the printable version of the each key spec is the same the key spec.
     */
    @Test
    public void testProviderShouldFormatPrintableStrings() {
        for (String keySpec : getKeySpecs(transitKeyId)) {
            String printable = VaultSensitivePropertyProvider.toPrintableString(keySpec);
            Assert.assertEquals(printable, keySpec);
            logger.info("Key spec:" + keySpec + " has printable:" + printable);
        }
    }

    /**
     * This test shows that the Vault SPP returns identifier keys that equal the original key spec used to create the SPP.
     */
    @Test
    public void testProviderIdentifierKey() {
        // We need some environment bits to create the SPP below:
        setTokenAuthProps();

        for (String keySpec : getKeySpecs(transitKeyId)) {
            SensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keySpec);
            Assert.assertEquals(keySpec, sensitivePropertyProvider.getIdentifierKey());
            logger.info("Key spec:" + keySpec + " has identifier key:" + sensitivePropertyProvider.getIdentifierKey());
        }
    }

    /**
     * This test shows that the Vault SPP is the SPP for our vault key specs.
     */
    @Test
    public void testProviderIsProviderForKeySpecs() {
        for (String keySpec : getKeySpecs(transitKeyId)) {
            Assert.assertTrue(VaultSensitivePropertyProvider.isProviderFor(keySpec));
        }
    }

    /**
     * This test shows that the Vault SPP is not the SPP for a variety of non-vault key specs.
     */
    @Test
    public void testProviderIsNotProviderForBadKeySpecs() {
        Set<String> malformedMaterials = Stream.of(
                null,
                "",
                " ",
                "\n",
                "\t \n",
                "vault/noauth",
                "vault",
                "vault/",
                "vault/something/something"
        ).collect(Collectors.toSet());

        for (String malformedMaterial : malformedMaterials) {
            Assert.assertFalse(VaultSensitivePropertyProvider.isProviderFor(malformedMaterial));
        }
    }

    @NotNull
    private Set<String> getKeySpecs(String keyId) {
        return Stream.of(
                VaultSensitivePropertyProvider.formatForAppIdAuth(keyId),
                VaultSensitivePropertyProvider.formatForAppRoleAuth(keyId),
                VaultSensitivePropertyProvider.formatForCubbyholeAuth(keyId),
                VaultSensitivePropertyProvider.formatForTokenAuth(keyId)
        ).collect(Collectors.toSet());
    }

    /**
     * This test shows that the provider can encrypt and decrypt values as expected.
     */
    @Test
    public void testProtectAndUnprotect() {
        setTokenAuthProps();

        for (String keyId : getKeySpecs(transitKeyId)) {
            SensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
            int plainSize = CipherUtils.getRandomInt(32, 256);
            checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
            logger.info("Vault SPP protected and unprotected string of " + plainSize + " bytes using material: " + keyId);
        }
    }

    /**
     * These tests show that the provider cannot encrypt empty values.
     */
    @Test
    public void testShouldHandleProtectEmptyValue() throws Exception {
        setTokenAuthProps();

        for (String keyId : getKeySpecs(transitKeyId)) {
            SensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
            checkProviderProtectDoesNotAllowBlankValues(sensitivePropertyProvider);
        }
    }

    /**
     * These tests show that the provider cannot decrypt invalid ciphertext.
     */
    @Test
    public void testShouldUnprotectValue() throws Exception {
        setTokenAuthProps();

        for (String keyId : getKeySpecs(transitKeyId)) {
            SensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
            checkProviderUnprotectDoesNotAllowInvalidBase64Values(sensitivePropertyProvider);
        }
    }

    /**
     * These tests show that the provider cannot decrypt text encoded but not encrypted.
     */
    @Test
    public void testShouldThrowExceptionWithValidBase64EncodedTextInvalidCipherText() throws Exception {
        setTokenAuthProps();

        for (String keyId : getKeySpecs(transitKeyId)) {
            SensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
            checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(sensitivePropertyProvider);
        }
    }

    /**
     * These tests show we can use an AWS KMS key to encrypt/decrypt property values.
     */
    @Test
    public void testShouldProtectAndUnprotectProperties() throws Exception {
        setTokenAuthProps();

        for (String keyId : getKeySpecs(transitKeyId)) {
            SensitivePropertyProvider sensitivePropertyProvider = new VaultSensitivePropertyProvider(keyId);
            checkProviderCanProtectAndUnprotectProperties(sensitivePropertyProvider);
        }
    }

    private void setTokenAuthProps() {
        System.setProperty("vault.authentication", VaultSensitivePropertyProvider.VAULT_AUTH_TOKEN);
        System.setProperty("vault.uri", vaultUri);
        System.setProperty("vault.token", vaultToken);
    }
}
