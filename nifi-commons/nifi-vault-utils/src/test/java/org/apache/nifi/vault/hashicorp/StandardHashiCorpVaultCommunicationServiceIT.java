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
package org.apache.nifi.vault.hashicorp;

import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The simplest way to run this test is by installing Vault locally, then running:
 *
 * vault server -dev
 * vault secrets enable transit
 * vault secrets enable kv
 * vault write -f transit/keys/nifi
 *
 * Make note of the Root Token and create a properties file with the contents:
 * vault.token=[Root Token]
 *
 * Then, set the system property -Dvault.auth.properties to the file path of the above properties file when
 * running the integration test.
 */
public class StandardHashiCorpVaultCommunicationServiceIT {

    private static final String TRANSIT_KEY = "nifi";

    private HashiCorpVaultCommunicationService vcs;

    @BeforeEach
    public void init() {
        vcs = new StandardHashiCorpVaultCommunicationService(new HashiCorpVaultProperties.HashiCorpVaultPropertiesBuilder()
                .setAuthPropertiesFilename(System.getProperty("vault.auth.properties"))
                .setUri("http://127.0.0.1:8200")
                .build());
    }

    @Test
    public void testEncryptDecrypt() {
        this.runEncryptDecryptTest();
    }

    public void runEncryptDecryptTest() {
        String plaintext = "this is the plaintext";

        String ciphertext = vcs.encrypt(TRANSIT_KEY, plaintext.getBytes(StandardCharsets.UTF_8));

        byte[] decrypted = vcs.decrypt(TRANSIT_KEY, ciphertext);

        assertEquals(plaintext, new String(decrypted, StandardCharsets.UTF_8));
    }

    /**
     * Run <code>vault kv get kv/key</code> to see the secret
     */
    @Test
    public void testReadWriteSecret() {
        final String key = "key";
        final String value = "value";

        vcs.writeKeyValueSecret("kv", key, value);

        final String resultValue = vcs.readKeyValueSecret("kv", key).orElseThrow(() -> new NullPointerException("Missing secret for kv/key"));
        assertEquals(value, resultValue);
    }

    /**
     * Run <code>vault kv get kv/secret</code> to see the secret
     */
    @Test
    public void testReadWriteSecretMap() {
        final String secretKey = "secret";
        final String key = "key";
        final String value = "value";
        final String key2 = "key2";
        final String value2 = "value2";

        final Map<String, String> keyValues = new HashMap<>();
        keyValues.put(key, value);
        keyValues.put(key2, value2);

        vcs.writeKeyValueSecretMap("kv", secretKey, keyValues);

        final Map<String, String> resultMap = vcs.readKeyValueSecretMap("kv", secretKey);
        assertEquals(keyValues, resultMap);
    }
}
