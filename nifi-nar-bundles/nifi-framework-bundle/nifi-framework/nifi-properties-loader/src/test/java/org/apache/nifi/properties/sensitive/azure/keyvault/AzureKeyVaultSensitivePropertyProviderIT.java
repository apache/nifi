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
package org.apache.nifi.properties.sensitive.azure.keyvault;

import org.apache.nifi.properties.sensitive.AbstractSensitivePropertyProviderTest;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Azure Key Vault Sensitive Property Provider.
 *
 * These tests rely on an environment with Azure credentials stored as files identified by environment variables, and require that those credentials support
 * creating and using Key Vault keys.
 *
 * These tests are more narrow than other Sensitive Property Provider tests in that we don't construct keys or key material,
 * and instead rely on user values completely.  Because of that, no user keys are created or destroyed.
 *
To exercise these tests, set the Azure environment variables like so:

 AZURE_AUTH_LOCATION=/var/run/secrets/nifi-azure-auth.json
 AZURE_KEY_VAULT_MATERIAL=/subscriptions/your-subscription-uuid/resourceGroups/your-resource-group/providers/Microsoft.KeyVault/vaults/your-vault,https://your-vault.vault.azure.net/keys/your-key/your-key-id

 Azure with Java:
 https://docs.microsoft.com/en-us/azure/java/?view=azure-java-stable

 Azure auth with Java:
 https://github.com/Azure/azure-libraries-for-java/blob/master/AUTH.md

 Azure Key Vault with Java:
 https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.keyvault?view=azure-java-stable
 */
public class AzureKeyVaultSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(AzureKeyVaultSensitivePropertyProviderIT.class);
    private SensitivePropertyProvider sensitivePropertyProvider;

    @BeforeClass
    public static void checkAssumptions() {
        Assume.assumeTrue(System.getenv("AZURE_AUTH_LOCATION") != null);
    }

    @Before
    public void createSensitivePropertyProvider() {
        String material = System.getenv("AZURE_KEY_VAULT_MATERIAL");
        Assume.assumeTrue(material != null);
        sensitivePropertyProvider = new AzureKeyVaultSensitivePropertyProvider(material);
    }

    /**
     * These tests show that the provider can encrypt and decrypt values.
     */
    @Test
    public void testProtectAndUnprotect() throws Exception {
        int plainSize = 127;
        checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
        logger.info("Azure Key Vault SPP protected and unprotected string of " + plainSize);

        /*

         To determine max length, try something like this:

         for (int i = 127; i<570/2; i++) {
         checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, i);
         logger.info("Azure Key Vault SPP protected and unprotected string of " + i);
         }
         */
    }

    /**
     * These tests show that the provider cannot encrypt empty values.
     */
    @Test
    public void testShouldHandleProtectEmptyValue() throws Exception {
        checkProviderProtectDoesNotAllowBlankValues(sensitivePropertyProvider);
    }

    /**
     * These tests show that the provider cannot decrypt invalid ciphertext.
     */
    @Test
    public void testProviderUnprotectWithBadValues() throws Exception {
        checkProviderUnprotectDoesNotAllowInvalidBase64Values(sensitivePropertyProvider);
    }

    /**
     * These tests show that the provider cannot decrypt text encoded but not encrypted.
     */
    @Test
    public void testShouldThrowExceptionWithValidBase64EncodedTextInvalidCipherText() throws Exception {
        checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(sensitivePropertyProvider);
    }

    /**
     * These tests show that the provider can protect and unprotect properties.
     */
    @Test
    public void testShouldProtectAndUnprotectProperties() throws Exception {
        checkProviderCanProtectAndUnprotectProperties(sensitivePropertyProvider);
    }
}
