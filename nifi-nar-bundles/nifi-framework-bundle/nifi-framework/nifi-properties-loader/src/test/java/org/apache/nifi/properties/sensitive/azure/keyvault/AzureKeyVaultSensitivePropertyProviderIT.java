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


public class AzureKeyVaultSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(AzureKeyVaultSensitivePropertyProviderIT.class);
    private SensitivePropertyProvider spp;

    @BeforeClass
    public static void checkAssumptions() {
        Assume.assumeTrue(System.getenv("AZURE_AUTH_LOCATION") != null);
    }

    @Before
    public void createSensitivePropertyProvider() {
        String material = System.getenv("AZURE_KEY_VAULT_MATERIAL");
        Assume.assumeTrue(material != null);
        spp = new AzureKeyVaultSensitivePropertyProvider(material);
    }

    @Test
    public void testProtectAndUnprotect() throws Exception {
        int plainSize = 128;
        checkProviderCanProtectAndUnprotectValue(spp, plainSize);
        logger.info("Azure Key Vault SPP protected and unprotected string of " + plainSize);
    }

    /**
     * These tests show that the provider cannot encrypt empty values.
     */
    @Test
    public void testShouldHandleProtectEmptyValue() throws Exception {
        checkProviderProtectDoesNotAllowBlankValues(spp);
    }

    /**
     * These tests show that the provider cannot decrypt invalid ciphertext.
     */
    @Test
    public void testProviderUnprotectWithBadValues() throws Exception {
        checkProviderUnprotectDoesNotAllowInvalidBase64Values(spp);
    }

    /**
     * These tests show that the provider cannot decrypt text encoded but not encrypted.
     */
    @Test
    public void testShouldThrowExceptionWithValidBase64EncodedTextInvalidCipherText() throws Exception {
        checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(spp);
    }

    @Test
    public void testShouldProtectAndUnprotectProperties() throws Exception {
        checkProviderCanProtectAndUnprotectProperties(spp);
    }
}
