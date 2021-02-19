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

import org.junit.Test;

public class SecureHasherFactoryTest {

    private static final Argon2SecureHasher DEFAULT_HASHER = new Argon2SecureHasher();

    @Test
    public void testSecureHasherFactoryArgon2() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("NIFI_ARGON2_AES_GCM_128");
        assert(hasher instanceof Argon2SecureHasher);
    }

    @Test
    public void testSecureHasherFactoryPBKDF2() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("NIFI_PBKDF2_AES_GCM_128");
        assert(hasher instanceof PBKDF2SecureHasher);
    }

    @Test
    public void testSecureHasherFactoryBCrypt() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("NIFI_BCRYPT_AES_GCM_128");
        assert(hasher instanceof BcryptSecureHasher);
    }

    @Test
    public void testSecureHasherFactorySCrypt() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("NIFI_SCRYPT_AES_GCM_128");
        assert(hasher instanceof ScryptSecureHasher);
    }

    @Test
    public void testSecureHasherFactoryArgon2ShortName() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("ARGON2");
        assert(hasher instanceof Argon2SecureHasher);
    }

    @Test
    public void testSecureHasherFactorySCryptShortName() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("SCRYPT");
        assert(hasher instanceof ScryptSecureHasher);
    }

    @Test
    public void testSecureHasherFactoryArgon2SimilarName() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("ARGON_2");
        assert(hasher.getClass().isInstance(DEFAULT_HASHER));
    }

    @Test
    public void testSecureHasherFactorySCrypt3() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("SCRYPTA");
        assert(hasher.getClass().isInstance(DEFAULT_HASHER));
    }

    @Test
    public void testSecureHasherBadAlgorithmName() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("wrongString");
        assert(hasher.getClass().isInstance(DEFAULT_HASHER));
    }

    @Test
    public void testSecureHasherFactoryFailsWithUnknownAlgorithm() {
        SecureHasher hasher = SecureHasherFactory.getSecureHasher("NIFI_UNKNONWN_AES_GCM_256");
        assert(hasher.getClass().isInstance(DEFAULT_HASHER));
    }
}
