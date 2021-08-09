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
package org.apache.nifi.processors.pgp.attributes;

import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;

/**
 * PGP Symmetric-Key Algorithm Definitions supported for Encryption
 */
public enum SymmetricKeyAlgorithm {
    AES_128(BlockCipher.AES, 128, SymmetricKeyAlgorithmTags.AES_128),

    AES_192(BlockCipher.AES, 192, SymmetricKeyAlgorithmTags.AES_192),

    AES_256(BlockCipher.AES, 256, SymmetricKeyAlgorithmTags.AES_256),

    CAMELLIA_128(BlockCipher.CAMELLIA, 128, SymmetricKeyAlgorithmTags.CAMELLIA_128),

    CAMELLIA_192(BlockCipher.CAMELLIA, 192, SymmetricKeyAlgorithmTags.CAMELLIA_192),

    CAMELLIA_256(BlockCipher.CAMELLIA, 256, SymmetricKeyAlgorithmTags.CAMELLIA_256);

    private BlockCipher blockCipher;

    private int keySize;

    private int id;

    SymmetricKeyAlgorithm(final BlockCipher blockCipher, final int keySize, final int id) {
        this.blockCipher = blockCipher;
        this.keySize = keySize;
        this.id = id;
    }

    public BlockCipher getBlockCipher() {
        return blockCipher;
    }

    public int getKeySize() {
        return keySize;
    }

    public int getId() {
        return id;
    }
}
