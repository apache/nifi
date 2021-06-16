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
import java.util.HashMap;
import java.util.Map;

/**
 * <p> Provides a factory for SecureHasher implementations. Will return Argon2 by default if no algorithm parameter is given.
 * Algorithm parameter should align with the below registered secure hasher names (PBKDF2, BCRYPT, SCRYPT, ARGON2).
 */
public class SecureHasherFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecureHasherFactory.class);

    private static Map<KeyDerivationFunction, Class<? extends SecureHasher>> registeredSecureHashers;
    private static final Class<? extends SecureHasher> DEFAULT_SECURE_HASHER_CLASS = Argon2SecureHasher.class;

    static {
        registeredSecureHashers = new HashMap<>();
        registeredSecureHashers.put(KeyDerivationFunction.PBKDF2, PBKDF2SecureHasher.class);
        registeredSecureHashers.put(KeyDerivationFunction.BCRYPT, BcryptSecureHasher.class);
        registeredSecureHashers.put(KeyDerivationFunction.SCRYPT, ScryptSecureHasher.class);
        registeredSecureHashers.put(KeyDerivationFunction.ARGON2, Argon2SecureHasher.class);
    }

    public static SecureHasher getSecureHasher(final String algorithm) {
        Class<? extends SecureHasher> secureHasherClass = DEFAULT_SECURE_HASHER_CLASS;
        final String algorithmPattern = algorithm.toUpperCase();
        try {
            for (KeyDerivationFunction keyDerivationFunction : registeredSecureHashers.keySet()) {
                final String functionName = keyDerivationFunction.getKdfName().toUpperCase();
                if (algorithmPattern.contains(functionName)) {
                    secureHasherClass = registeredSecureHashers.get(keyDerivationFunction);
                }
            }
            LOGGER.debug("Creating SecureHasher [{}] for algorithm [{}]", secureHasherClass.getName(), algorithm);
            return secureHasherClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SecureHasherException(String.format("Failed to create SecureHasher for algorithm [%s]", algorithm), e);
        }
    }
}