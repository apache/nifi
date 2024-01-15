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
package org.apache.nifi.processors.cipher.compatibility;

import org.apache.nifi.processors.cipher.algorithm.SymmetricCipher;
import org.apache.nifi.processors.cipher.algorithm.DigestAlgorithm;
import org.apache.nifi.components.DescribedValue;

import static org.apache.nifi.processors.cipher.algorithm.SymmetricCipher.AES;
import static org.apache.nifi.processors.cipher.algorithm.SymmetricCipher.DES;
import static org.apache.nifi.processors.cipher.algorithm.SymmetricCipher.DESEDE;
import static org.apache.nifi.processors.cipher.algorithm.SymmetricCipher.RC2;
import static org.apache.nifi.processors.cipher.algorithm.SymmetricCipher.RC4;
import static org.apache.nifi.processors.cipher.algorithm.SymmetricCipher.TWOFISH;

/**
 * Compatibility Mode Encryption Schemes supporting decryption using legacy algorithms such as PBES1 defined in RFC 8018 Section 6.1
 */
public enum CompatibilityModeEncryptionScheme implements DescribedValue {
    PBE_WITH_MD5_AND_AES_CBC_128(
            "PBEWITHMD5AND128BITAES-CBC-OPENSSL",
            DigestAlgorithm.MD5,
            AES,
            "PKCS12 with MD5 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 128 bit keys."
    ),

    PBE_WITH_MD5_AND_AES_CBC_192(
            "PBEWITHMD5AND192BITAES-CBC-OPENSSL",
            DigestAlgorithm.MD5,
            AES,
            "PKCS12 with MD5 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 192 bit keys."
    ),

    PBE_WITH_MD5_AND_AES_CBC_256(
            "PBEWITHMD5AND256BITAES-CBC-OPENSSL",
            DigestAlgorithm.MD5,
            AES,
            "PKCS12 with MD5 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 256 bit keys."
    ),

    PBE_WITH_MD5_AND_DES(
            "PBEWITHMD5ANDDES",
            DigestAlgorithm.MD5,
            DES,
            "PKCS5 Scheme 1 with MD5 digest and Data Encryption Standard 64 bit keys. OID 1.2.840.113549.1.5.3"
    ),

    PBE_WITH_MD5_AND_RC2(
            "PBEWITHMD5ANDRC2",
            DigestAlgorithm.MD5,
            RC2,
            "PKCS Scheme 1 with MD5 digest and Rivest Cipher 2. OID 1.2.840.113549.1.5.6"
    ),

    PBE_WITH_SHA1_AND_AES_CBC_128(
            "PBEWITHSHAAND128BITAES-CBC-BC",
            DigestAlgorithm.SHA1,
            AES,
            "PKCS12 with SHA-1 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 128 bit keys."
    ),

    PBE_WITH_SHA1_AND_AES_CBC_192(
            "PBEWITHSHAAND192BITAES-CBC-BC",
            DigestAlgorithm.SHA1,
            AES,
            "PKCS12 with SHA-1 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 192 bit keys."
    ),

    PBE_WITH_SHA1_AND_AES_CBC_256(
            "PBEWITHSHAAND256BITAES-CBC-BC",
            DigestAlgorithm.SHA1,
            AES,
            "PKCS12 with SHA-1 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 256 bit keys."
    ),

    PBE_WITH_SHA1_AND_DES(
            "PBEWITHSHA1ANDDES",
            DigestAlgorithm.SHA1,
            DES,
            "PKCS5 Scheme 1 with SHA-1 digest and Data Encryption Standard. OID 1.2.840.113549.1.5.10"
    ),

    PBE_WITH_SHA1_AND_DESEDE_128(
            "PBEWITHSHAAND2-KEYTRIPLEDES-CBC",
            DigestAlgorithm.SHA1,
            DESEDE,
            "PKCS12 with SHA-1 digest and Triple Data Encryption Standard 128 bit keys. OID 1.2.840.113549.1.12.1.4"
    ),

    PBE_WITH_SHA1_AND_DESEDE_192(
            "PBEWITHSHAAND3-KEYTRIPLEDES-CBC",
            DigestAlgorithm.SHA1,
            DESEDE,
            "PKCS12 with SHA-1 digest and Triple Data Encryption Standard 192 bit keys. OID 1.2.840.113549.1.12.1.3"
    ),

    PBE_WITH_SHA1_AND_RC2(
            "PBEWITHSHA1ANDRC2",
            DigestAlgorithm.SHA1,
            RC2,
            "PKCS5 Scheme 1 with SHA-1 digest and Rivest Cipher 2. OID 1.2.840.113549.1.5.11"
    ),

    PBE_WITH_SHA1_AND_RC2_128(
            "PBEWITHSHAAND128BITRC2-CBC",
            DigestAlgorithm.SHA1,
            RC2,
            "PKCS12 with SHA-1 digest and Rivest Cipher 2 128 bit keys. OID 1.2.840.113549.1.12.1.5"
    ),

    PBE_WITH_SHA1_AND_RC2_40(
            "PBEWITHSHAAND40BITRC2-CBC",
            DigestAlgorithm.SHA1,
            RC2,
            "PKCS12 with SHA-1 digest and Rivest Cipher 2 40 bit keys. OID 1.2.840.113549.1.12.1.6"
    ),

    PBE_WITH_SHA1_AND_RC4_128(
            "PBEWITHSHAAND128BITRC4",
            DigestAlgorithm.SHA1,
            RC4,
            "PKCS12 with SHA-1 digest and Rivest Cipher 4 128 bit keys. OID 1.2.840.113549.1.12.1.1"
    ),

    PBE_WITH_SHA1_AND_RC4_40(
            "PBEWITHSHAAND40BITRC4",
            DigestAlgorithm.SHA1,
            RC4,
            "PKCS12 with SHA-1 digest and Rivest Cipher 4 40 bit keys. OID 1.2.840.113549.1.12.1.2"
    ),

    PBE_WITH_SHA1_AND_TWOFISH(
            "PBEWITHSHAANDTWOFISH-CBC",
            DigestAlgorithm.SHA1,
            TWOFISH,
            "PKCS12 with SHA-1 digest and Twofish in Cipher Block Chaining mode using 256 bit keys."
    ),

    PBE_WITH_SHA256_AND_AES_CBC_128(
            "PBEWITHSHA256AND128BITAES-CBC-BC",
            DigestAlgorithm.SHA256,
            AES,
            "PKCS12 with SHA-256 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 128 bit keys."
    ),

    PBE_WITH_SHA256_AND_AES_CBC_192(
            "PBEWITHSHA256AND192BITAES-CBC-BC",
            DigestAlgorithm.SHA256,
            AES,
            "PKCS12 with SHA-256 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 192 bit keys."
    ),

    PBE_WITH_SHA256_AND_AES_CBC_256(
            "PBEWITHSHA256AND256BITAES-CBC-BC",
            DigestAlgorithm.SHA256,
            AES,
            "PKCS12 with SHA-256 digest and Advanced Encryption Standard in Cipher Block Chaining mode using 256 bit keys."
    );

    private final String algorithm;
    private final String description;
    private final DigestAlgorithm digestAlgorithm;
    private final SymmetricCipher symmetricCipher;

    CompatibilityModeEncryptionScheme(
            final String algorithm,
            final DigestAlgorithm digestAlgorithm,
            final SymmetricCipher symmetricCipher,
            final String description
    ) {
        this.algorithm = algorithm;
        this.description = description;
        this.digestAlgorithm = digestAlgorithm;
        this.symmetricCipher = symmetricCipher;
    }

    @Override
    public String getValue() {
        return algorithm;
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public SymmetricCipher getSymmetricCipher() {
        return symmetricCipher;
    }
}
