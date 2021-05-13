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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * The simplest way to run this test is by installing Vault locally, then running:
 *
 * vault server -dev
 * vault secrets enable transit
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

    @Before
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

        Assert.assertEquals(plaintext, new String(decrypted, StandardCharsets.UTF_8));
    }
}
