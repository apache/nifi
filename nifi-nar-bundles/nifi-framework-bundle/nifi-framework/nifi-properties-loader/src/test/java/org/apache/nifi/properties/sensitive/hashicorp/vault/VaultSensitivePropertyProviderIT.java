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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.vault.VaultException;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.authentication.IpAddressUserId;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultOperations;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultMount;
import org.springframework.vault.support.VaultResponse;
import org.springframework.vault.support.WrappedMetadata;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import java.net.URI;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@RunWith(JUnit4.class)
public class VaultSensitivePropertyProviderIT  {
    private static final Logger logger = LoggerFactory.getLogger(VaultSensitivePropertyProviderIT.class);

    private static final String VAULT_APP_ID = "unit-test";
    private static final String VAULT_ROLE_NAME = VAULT_APP_ID;
    private static final SecureRandom random = new SecureRandom();

    private static GenericContainer vaultContainer;
    private static VaultOperations initClient;

    private static Integer serverPort = 8200;
    private static String serverUri;

    // For token auth tests:
    private static String authToken;

    // For app role auth tests:
    private static String authRoleId;
    private static String authSecretId;

    // For app id auth tests:
    private static String authAppId;
    private static String authUserId;

    // For tests of all the auth types:
    private static Set<String> clientMaterials;

    /**
     * This method creates the Vault environment needed by the test cases.  It checks for VAULT_TOKEN and VAULT_ADDR
     * system properties, and in their absence, runs a Vault server in a test container.  In both cases, the method
     * configures app role and app id auth.
     *
     */
    @BeforeClass
    public static void createVaultTestEnvironment() {
        String tokenProp = System.getProperty("VAULT_TOKEN");
        String uriProp = System.getProperty("VAULT_ADDR");

        if (StringUtils.isNotBlank(tokenProp)) {
            authToken = tokenProp;
        }
        if (StringUtils.isNotBlank(uriProp)) {
            serverUri = uriProp;
        }

        if (StringUtils.isBlank(serverUri)) {
            authToken = getRandomHex(16);
            vaultContainer = generateTestContainer(authToken, serverPort);
            vaultContainer.start();

            // Tests are HTTP for now.  :(
            serverUri = "http://" + vaultContainer.getContainerIpAddress() + ":" + vaultContainer.getMappedPort(serverPort);
        }

        // We need a client to initialize the vault server, and it's not the client of the SPP.
        VaultEndpoint endpoint = VaultEndpoint.from(URI.create(serverUri));
        ClientAuthentication auth = new TokenAuthentication(authToken);
        VaultOperations client = new VaultTemplate(endpoint, auth);

        try {
            client.opsForSys().authMount("app-id", VaultMount.create("app-id"));
            client.opsForSys().authMount("approle", VaultMount.create("approle"));
        } catch (final VaultException ignored) {
            // Ignore errors in case the auth mounts already exist.
        }

        String rules = "{'name': 'test-policy', 'path': {'*': {'policy': 'read'}}}";
        rules = rules.replace("'", "\"");
        client.write("sys/policy/test-policy", Collections.singletonMap("rules", rules));

        Map<String, String> config = new HashMap<>();
        config.put("policies", "test-policy");
        config.put("bound_cidr_list", "0.0.0.0/0");
        config.put("bind_secret_id", "true");
        client.write("auth/approle/role/" + VAULT_ROLE_NAME, config);

        authRoleId = (String) client.read(String.format("auth/approle/role/%s/role-id", VAULT_ROLE_NAME)).getData().get("role_id");
        authSecretId = (String) client.write(String.format("auth/approle/role/%s/secret-id", VAULT_ROLE_NAME)).getData().get("secret_id");
        logger.info("Constructed approle auth with role-id:" + authRoleId + " and secret-id:" + authSecretId);

        config = new HashMap<>();
        config.put("policies", "test-policy");
        config.put("display_name", VAULT_APP_ID);
        client.write("auth/app-id/map/app-id/" + VAULT_APP_ID, config);
        authUserId = new IpAddressUserId().createUserId();

        config = new HashMap<>();
        config.put("value", VAULT_APP_ID);
        config.put("cidr_block", "0.0.0.0/0");
        client.write("auth/app-id/map/user-id/" + authUserId, config);
        logger.info("Constructed app-id auth with app-id:" + VAULT_APP_ID + " user-id:" + authUserId);

        // Construct a set of client materials, one for each known auth type:
        clientMaterials = Stream.of(
                VaultSensitivePropertyProvider.formatForToken(serverUri, authToken),
                VaultSensitivePropertyProvider.formatForAppRole(serverUri, authRoleId, authSecretId),
                VaultSensitivePropertyProvider.formatForAppId(serverUri, VAULT_APP_ID, authUserId)
        ).collect(Collectors.toSet());

        initClient = client; // Expose the client to the baseline check
    }

    @AfterClass
    public static void tearDownOnce() {
        try {
            vaultContainer.stop();
        } catch (final Exception ignored) {
        }
    }

    /**
     * This test shows that the Vault wrap and unwrap calls behave like we expect them to behave:  we can wrap a value
     * with a key, and we can unwrap a previously wrapped value.
     */
    @Test
    public void testPreFlightClientShouldWrapAndUnwrapDirectly() throws Exception {
        Map<String, String> data = new HashMap<String, String>();
        int dataSize = 0;
        int keyCount = getRandomInt(12, 64);

        for (int i = 0; i < keyCount; i++) {
            int keySize = getRandomInt(4, 32);
            int valueSize = getRandomInt(32, 256);
            data.put(getRandomHex(keySize), getRandomHex(valueSize));
            dataSize += keySize + valueSize;
        }

        WrappedMetadata wrapped = VaultSensitivePropertyProvider.vaultWrap(initClient, data, Duration.ofDays(365));
        VaultResponse unwrapped = VaultSensitivePropertyProvider.vaultUnwrap(initClient, wrapped.getToken().getToken());
        Assert.assertEquals(unwrapped.getData(), data);

        logger.info("Wrapped, unwrapped, and matched map of " + dataSize + " random bytes during initial checks.");
    }

    /**
     * This test shows that we can specify various types of authentication when we construct the property provider,
     * and each of those types gives a provider that wraps and unwraps text as expected.
     */
    @Test
    public void testProviderShouldProtectAndUnprotectWithAllAuthTypes() throws Exception {
        for (String clientMaterial : clientMaterials) {
            VaultSensitivePropertyProvider spp = new VaultSensitivePropertyProvider(clientMaterial);
            int plainSize = getRandomInt(32, 256);

            String plainText = getRandomHex(plainSize);
            String cipherText = spp.protect(plainText);

            Assert.assertNotNull(cipherText);
            Assert.assertNotEquals(plainText, cipherText);

            String unwrappedText = spp.unprotect(cipherText);
            Assert.assertNotNull(unwrappedText);
            Assert.assertEquals(unwrappedText, plainText);

            logger.info("Protected and unprotected string of " + plainSize + " bytes using material: " + clientMaterial);
        }
    }

    /**
     * This test shows that all of the clients throw an exception on empty plaintext.
     */
    @Test
    public void testProviderShouldThrowExceptionWithEmptyValue() {
        for (String clientMaterial : clientMaterials) {
            VaultSensitivePropertyProvider spp = new VaultSensitivePropertyProvider(clientMaterial);

            Boolean failed = false;
            try  {
                String cipherText = spp.protect("");
            } catch (final IllegalArgumentException ignored) {
                failed = true;
            }

            Assert.assertTrue(failed);
        }
    }

    /**
     * This test shows the printable version of the client materials is different.
     */
    @Test
    public void testProviderShouldFormatPrintableStrings() {
        for (String clientMaterial : clientMaterials) {
            String printable = VaultSensitivePropertyProvider.toPrintableString(clientMaterial);
            Assert.assertNotEquals(printable, clientMaterial);
            Assert.assertTrue(printable.startsWith(VaultSensitivePropertyProvider.VAULT_PREFIX + VaultSensitivePropertyProvider.VAULT_KEY_DELIMITER));

            logger.info("From client material: " + clientMaterial + " now have printable: " + printable);
        }
    }

    @Test
    public void testProviderIdentifierKey() {
        for (String clientMaterial : clientMaterials) {
            SensitivePropertyProvider spp = new VaultSensitivePropertyProvider(clientMaterial);
            Assert.assertEquals(clientMaterial, spp.getIdentifierKey());
        }
    }


    @Test
    public void testProviderIsProviderForMaterial() {
        for (String clientMaterial : clientMaterials) {
            Assert.assertTrue(VaultSensitivePropertyProvider.isProviderFor(clientMaterial));
        }
    }


    @Test
    public void testProviderIsNotProviderForBadMaterial() {
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

    private static GenericContainer generateTestContainer(String token, Integer port) {
        return new GenericContainer<>("vault:latest")
                .withExposedPorts(port)
                .withEnv("VAULT_DEV_ROOT_TOKEN_ID", token)
                .waitingFor(Wait.forLogMessage("==> Vault server started! .*", 1));
    }

    private static String getRandomHex(int size) {
        byte[] tokenBytes = new byte[size];
        random.nextBytes(tokenBytes);
        return Hex.toHexString(tokenBytes);
    }

    private static int getRandomInt(int lower, int upper) {
        int value = random.nextInt(upper);
        while (value < lower) {
            value = random.nextInt(upper);
        }
        return value;
    }
}
