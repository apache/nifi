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
package org.apache.nifi.security.util.crypto;

import org.apache.nifi.security.util.KeyDerivationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SecureHasherFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecureHasherFactory.class);

    private static Map<String, Class<? extends SecureHasher>> registeredSecureHashers;
    private static final String DEFAULT_HASHER = KeyDerivationFunction.ARGON2.getKdfName().toUpperCase();

    static {
        registeredSecureHashers = new HashMap<>();
        registeredSecureHashers.put(KeyDerivationFunction.PBKDF2.getKdfName().toUpperCase(), PBKDF2SecureHasher.class);
        registeredSecureHashers.put(KeyDerivationFunction.BCRYPT.getKdfName().toUpperCase(), BcryptSecureHasher.class);
        registeredSecureHashers.put(KeyDerivationFunction.SCRYPT.getKdfName().toUpperCase(), ScryptSecureHasher.class);
        registeredSecureHashers.put(KeyDerivationFunction.ARGON2.getKdfName().toUpperCase(), Argon2SecureHasher.class);
    }

    public static SecureHasher getSecureHasher() {
        return getSecureHasher(DEFAULT_HASHER);
    }

    public static SecureHasher getSecureHasher(final String algorithm) {
        try {
            String hasherName = getHasherAlgorithm(algorithm);
            Class<? extends SecureHasher> clazz = registeredSecureHashers.get(hasherName);
            LOGGER.debug("Initializing secure hasher {}", clazz.getCanonicalName());
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SecureHasherException("Could not instantiate a valid SecureHasher implementation.");
        }
    }

    private static String getHasherAlgorithm(final String algorithm) throws ConfigurationException {
        final String hashingName = getHashingName(algorithm);

        Optional<String> chosenKdf = registeredSecureHashers.keySet().stream().filter(keyDerivationFunction -> keyDerivationFunction.equals(hashingName)).findFirst();

        if(chosenKdf.isPresent()) {
            return chosenKdf.get();
        } else {
            // Default to Argon2
            LOGGER.debug("Secure hasher implementation for {} not found, defaulting secure hasher to {}", algorithm, DEFAULT_HASHER);
            return DEFAULT_HASHER;
        }
    }

    private static String getHashingName(final String algorithm) {
        List<String> algorithmParts = Arrays.asList(algorithm.split("_"));
        Optional<String> algoName = algorithmParts.stream().filter(item -> registeredSecureHashers.containsKey(item.toUpperCase())).findFirst();

        return algoName.isPresent() ? algoName.get() : null;
    }

}
