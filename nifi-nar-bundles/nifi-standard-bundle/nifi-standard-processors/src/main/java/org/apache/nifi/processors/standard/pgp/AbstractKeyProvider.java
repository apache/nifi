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
package org.apache.nifi.processors.standard.pgp;

import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;

/**
 * This is a shared base class for other key provider classes, e.g., test provider and static value provider.
 */
class AbstractKeyProvider implements KeyProvider {
    PGPPublicKey publicKey;
    PGPSecretKey secretKey;
    PGPPrivateKey privateKey;

    void init(PGPPublicKey publicKey, PGPSecretKey secretKey, PGPPrivateKey privateKey) {
        this.publicKey = publicKey;
        this.secretKey = secretKey;
        this.privateKey = privateKey;
    }

    /**
     * Returns the public key, if any.
     *
     * @return provider public key
     */
    @Override
    public PGPPublicKey getPublicKey() {
        return publicKey;
    }

    /**
     * Returns the secret key, if any.
     *
     * @return provider secret key
     */
    @Override
    public PGPSecretKey getSecretKey() {
        return secretKey;
    }

    /**
     * Returns the private key, if any.
     *
     * @return provider private key
     */
    @Override
    public PGPPrivateKey getPrivateKey() {
        return privateKey;
    }
}
