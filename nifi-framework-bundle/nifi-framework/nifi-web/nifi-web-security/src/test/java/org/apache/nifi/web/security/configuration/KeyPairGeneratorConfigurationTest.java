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
package org.apache.nifi.web.security.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KeyPairGeneratorConfigurationTest {
    private static final String STANDARD_KEY_PAIR_ALGORITHM_FILTER = "KeyPairGenerator.Ed25519";

    private static final String STANDARD_KEY_PAIR_ALGORITHM = "Ed25519";

    private static final String FALLBACK_KEY_PAIR_ALGORITHM = "RSA";

    private KeyPairGeneratorConfiguration configuration;

    @BeforeEach
    void setConfiguration() {
        configuration = new KeyPairGeneratorConfiguration();
    }

    @Test
    void testJwtKeyPairGenerator() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = configuration.jwtKeyPairGenerator();

        final String algorithm = keyPairGenerator.getAlgorithm();
        assertEquals(STANDARD_KEY_PAIR_ALGORITHM, algorithm);
    }

    @Test
    void testJwtKeyPairGeneratorFallbackAlgorithm() throws NoSuchAlgorithmException {
        final Provider[] providers = Security.getProviders(STANDARD_KEY_PAIR_ALGORITHM_FILTER);
        assertNotNull(providers);

        final Provider provider = providers[0];
        try {
            Security.removeProvider(provider.getName());

            final KeyPairGenerator keyPairGenerator = configuration.jwtKeyPairGenerator();

            final String algorithm = keyPairGenerator.getAlgorithm();
            assertEquals(FALLBACK_KEY_PAIR_ALGORITHM, algorithm);
        } finally {
            Security.addProvider(provider);
        }
    }
}
