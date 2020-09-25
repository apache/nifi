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
package org.apache.nifi.processors.standard.crypto.algorithm;

import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.bouncycastle.asn1.ntt.NTTObjectIdentifiers;
import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;

/**
 * Cryptographic Algorithm enumerates identified ciphers and modes
 */
public enum CryptographicAlgorithm {
    AES_128_CBC(CryptographicCipher.AES, 128, BlockCipherMode.CBC, NISTObjectIdentifiers.id_aes128_CBC.getId()),

    AES_128_CCM(CryptographicCipher.AES, 128, BlockCipherMode.CCM, NISTObjectIdentifiers.id_aes128_CCM.getId()),

    AES_128_GCM(CryptographicCipher.AES, 128, BlockCipherMode.GCM, NISTObjectIdentifiers.id_aes128_GCM.getId()),

    AES_192_CBC(CryptographicCipher.AES, 192, BlockCipherMode.CBC, NISTObjectIdentifiers.id_aes192_CBC.getId()),

    AES_192_CCM(CryptographicCipher.AES, 192, BlockCipherMode.CCM, NISTObjectIdentifiers.id_aes192_CCM.getId()),

    AES_192_GCM(CryptographicCipher.AES, 192, BlockCipherMode.GCM, NISTObjectIdentifiers.id_aes192_GCM.getId()),

    AES_256_CBC(CryptographicCipher.AES, 256, BlockCipherMode.CBC, NISTObjectIdentifiers.id_aes256_CBC.getId()),

    AES_256_CCM(CryptographicCipher.AES, 256, BlockCipherMode.CCM, NISTObjectIdentifiers.id_aes256_CCM.getId()),

    AES_256_GCM(CryptographicCipher.AES, 256, BlockCipherMode.GCM, NISTObjectIdentifiers.id_aes256_GCM.getId()),

    CAMELLIA_128_CBC(CryptographicCipher.CAMELLIA, 128, BlockCipherMode.CBC, NTTObjectIdentifiers.id_camellia128_cbc.getId()),

    CAMELLIA_192_CBC(CryptographicCipher.CAMELLIA, 192, BlockCipherMode.CBC, NTTObjectIdentifiers.id_camellia192_cbc.getId()),

    CAMELLIA_256_CBC(CryptographicCipher.CAMELLIA, 256, BlockCipherMode.CBC, NTTObjectIdentifiers.id_camellia256_cbc.getId()),

    DES_56_CBC(CryptographicCipher.DES, 56, BlockCipherMode.CBC, OIWObjectIdentifiers.desCBC.getId()),

    RC2_40_CBC(CryptographicCipher.RC2, 40, BlockCipherMode.CBC, PKCSObjectIdentifiers.RC2_CBC.getId()),

    TDEA_168_CBC(CryptographicCipher.TDEA, 168, BlockCipherMode.CBC, PKCSObjectIdentifiers.des_EDE3_CBC.getId());

    private static final String FORMAT = "%s-%d-%s";

    private CryptographicCipher cipher;

    private int keySize;

    private BlockCipherMode blockCipherMode;

    private String objectIdentifier;

    CryptographicAlgorithm(final CryptographicCipher cipher, final int keySize, final BlockCipherMode blockCipherMode, final String objectIdentifier) {
        this.cipher = cipher;
        this.keySize = keySize;
        this.blockCipherMode = blockCipherMode;
        this.objectIdentifier = objectIdentifier;
    }

    public CryptographicCipher getCipher() {
        return cipher;
    }

    public int getKeySize() {
        return keySize;
    }

    public BlockCipherMode getBlockCipherMode() {
        return blockCipherMode;
    }

    public String getObjectIdentifier() {
        return objectIdentifier;
    }

    @Override
    public String toString() {
        return String.format(FORMAT, cipher.getLabel(), keySize, blockCipherMode.getLabel());
    }
}
