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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;

@Configuration
public class KeyPairGeneratorConfiguration {
    /** Standard Key Pair Algorithm for signing JSON Web Tokens */
    private static final String STANDARD_KEY_PAIR_ALGORITHM = "Ed25519";

    private static final String STANDARD_KEY_PAIR_ALGORITHM_FILTER = "KeyPairGenerator.Ed25519";

    /** Fallback Key Pair Algorithm when standard algorithm not supported in current Security Provider */
    private static final String FALLBACK_KEY_PAIR_ALGORITHM = "RSA";

    private static final Logger logger = LoggerFactory.getLogger(KeyPairGeneratorConfiguration.class);

    /**
     * JSON Web Token Key Pair Generator defaults to Ed25519 and falls back to RSA when current Security Providers do
     * not support Ed25519. The fallback strategy supports security configurations that have not included Ed25519
     * as an approved algorithm. This strategy works with restricted providers such as those that have not incorporated
     * algorithm approvals described in FIPS 186-5
     *
     * @return Key Pair Generator for JSON Web Token signing
     * @throws NoSuchAlgorithmException Thrown on failure to get Key Pair Generator for selected algorithm
     */
    @Bean
    public KeyPairGenerator jwtKeyPairGenerator() throws NoSuchAlgorithmException {
        final String keyPairAlgorithm;

        final Provider[] providers = Security.getProviders(STANDARD_KEY_PAIR_ALGORITHM_FILTER);
        if (providers == null) {
            keyPairAlgorithm = FALLBACK_KEY_PAIR_ALGORITHM;
        } else {
            keyPairAlgorithm = STANDARD_KEY_PAIR_ALGORITHM;
        }

        logger.info("Configured Key Pair Algorithm [{}] for JSON Web Signatures", keyPairAlgorithm);
        return KeyPairGenerator.getInstance(keyPairAlgorithm);
    }
}
