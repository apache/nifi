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
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.security.util.CipherUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCPKMSSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(GCPKMSSensitivePropertyProviderIT.class);
    //         "projects/P/locations/L/keyRings/R/cryptoKeys/K" {

    @Test
    public void showThatItWorks() {
        String keyId = "gcp/kms/nifi-gcp-unit-tests-project/us-west2/key-ring-0/key-name-0x037af";
        SensitivePropertyProvider sensitivePropertyProvider = new GCPKMSSensitivePropertyProvider(keyId);
        int plainSize = CipherUtils.getRandomInt(32, 256);

        checkProvider(sensitivePropertyProvider, plainSize);
        logger.info("GCP unprotected string of " + plainSize + " bytes using key id: " + keyId);
        // Assert.assertNotNull(sensitivePropertyProvider);
        // String plainText = CipherUtils.getRandomHex(64);
        // String cipherText = sensitivePropertyProvider.protect(plainText);
        // Assert.assertNotEquals(plainText, cipherText);
        // Assert.assertEquals(plainText, sensitivePropertyProvider.unprotect(cipherText));
    }
}