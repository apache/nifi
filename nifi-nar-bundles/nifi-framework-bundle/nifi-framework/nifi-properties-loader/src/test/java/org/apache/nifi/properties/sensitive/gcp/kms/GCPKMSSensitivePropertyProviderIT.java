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
package org.apache.nifi.properties.sensitive.gcp.kms;

import org.apache.nifi.properties.sensitive.AbstractSensitivePropertyProviderTest;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.security.util.CipherUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;


public class GCPKMSSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(GCPKMSSensitivePropertyProviderIT.class);
    private static final String keyId = "gcp/kms/nifi-gcp-unit-tests-project/us-west2/key-ring-0/key-name-0x037af";

    @BeforeClass
    public static void checkGcpEnvVar() {
        Assume.assumeTrue(System.getenv("GOOGLE_APPLICATION_CREDENTIALS") != null);
        // Assume.assumeTrue( ... running within GCE ... )
    }

    @Test
    public void testShouldThrowExceptionsWithBadKeys() throws Exception {
        try {
            new GCPKMSSensitivePropertyProvider("");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("The key cannot be empty").matcher(e.getMessage()).matches());
        }

        try {
            new GCPKMSSensitivePropertyProvider("this is an invalid key and will not work");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("Invalid GCP key").matcher(e.getMessage()).matches());
        }
    }

    @Test
    public void testProtectAndUnprotect() {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        int plainSize = CipherUtils.getRandomInt(32, 256);
        checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
        logger.info("GCP SPP protected and unprotected string of " + plainSize + " bytes using material: " + keyId);
    }

    /**
     * These tests show that the provider cannot encrypt empty values.
     */
    @Test
    public void testShouldHandleProtectEmptyValue() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderProtectDoesNotAllowBlankValues(sensitivePropertyProvider);
    }

    /**
     * These tests show that the provider cannot decrypt invalid ciphertext.
     */
    @Test
    public void testShouldUnprotectValue() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderUnprotectDoesNotAllowInvalidBase64Values(sensitivePropertyProvider);
    }

    /**
     * These tests show that the provider cannot decrypt text encoded but not encrypted.
     */
    @Test
    public void testShouldThrowExceptionWithValidBase64EncodedTextInvalidCipherText() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(sensitivePropertyProvider);
    }

    /**
     * These tests show we can use an AWS KMS key to encrypt/decrypt property values.
     */
    @Test
    public void testShouldProtectAndUnprotectProperties() throws Exception {
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        checkProviderCanProtectAndUnprotectProperties(sensitivePropertyProvider);
    }
}
