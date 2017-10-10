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
package org.apache.nifi.security.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Enumeration capturing essential information about the various encryption
 * methods that might be supported.
 */
public enum EncryptionMethod {

    MD5_128AES("PBEWITHMD5AND128BITAES-CBC-OPENSSL", "BC", false, false),
    MD5_192AES("PBEWITHMD5AND192BITAES-CBC-OPENSSL", "BC", true, false),
    MD5_256AES("PBEWITHMD5AND256BITAES-CBC-OPENSSL", "BC", true, false),
    MD5_DES("PBEWITHMD5ANDDES", "BC", false, false),
    MD5_RC2("PBEWITHMD5ANDRC2", "BC", false, false),
    SHA1_RC2("PBEWITHSHA1ANDRC2", "BC", false, false),
    SHA1_DES("PBEWITHSHA1ANDDES", "BC", false, false),
    SHA_128AES("PBEWITHSHAAND128BITAES-CBC-BC", "BC", false, false),
    SHA_192AES("PBEWITHSHAAND192BITAES-CBC-BC", "BC", true, false),
    SHA_256AES("PBEWITHSHAAND256BITAES-CBC-BC", "BC", true, false),
    SHA_40RC2("PBEWITHSHAAND40BITRC2-CBC", "BC", false, false),
    SHA_128RC2("PBEWITHSHAAND128BITRC2-CBC", "BC", false, false),
    SHA_40RC4("PBEWITHSHAAND40BITRC4", "BC", false, false),
    SHA_128RC4("PBEWITHSHAAND128BITRC4", "BC", false, false),
    SHA256_128AES("PBEWITHSHA256AND128BITAES-CBC-BC", "BC", false, false),
    SHA256_192AES("PBEWITHSHA256AND192BITAES-CBC-BC", "BC", true, false),
    SHA256_256AES("PBEWITHSHA256AND256BITAES-CBC-BC", "BC", true, false),
    SHA_2KEYTRIPLEDES("PBEWITHSHAAND2-KEYTRIPLEDES-CBC", "BC", false, false),
    SHA_3KEYTRIPLEDES("PBEWITHSHAAND3-KEYTRIPLEDES-CBC", "BC", false, false),
    SHA_TWOFISH("PBEWITHSHAANDTWOFISH-CBC", "BC", false, false),
    PGP("PGP", "BC", false, false),
    PGP_ASCII_ARMOR("PGP-ASCII-ARMOR", "BC", false, false),
    // New encryption methods which used keyed encryption
    AES_CBC("AES/CBC/PKCS7Padding", "BC", false, true),
    AES_CTR("AES/CTR/NoPadding", "BC", false, true),
    AES_GCM("AES/GCM/NoPadding", "BC", false, true);

    private final String algorithm;
    private final String provider;
    private final boolean unlimitedStrength;
    private final boolean compatibleWithStrongKDFs;

    EncryptionMethod(String algorithm, String provider, boolean unlimitedStrength, boolean compatibleWithStrongKDFs) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.unlimitedStrength = unlimitedStrength;
        this.compatibleWithStrongKDFs = compatibleWithStrongKDFs;
    }

    public String getProvider() {
        return provider;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    /**
     * @return true if algorithm requires unlimited strength policies
     */
    public boolean isUnlimitedStrength() {
        return unlimitedStrength;
    }

    /**
     * @return true if algorithm is compatible with strong {@link KeyDerivationFunction}s
     */
    public boolean isCompatibleWithStrongKDFs() {
        return compatibleWithStrongKDFs;
    }

    /**
     * @return true if this algorithm does not rely on its own internal key derivation process
     */
    public boolean isKeyedCipher() {
        return !algorithm.startsWith("PBE") && !algorithm.startsWith("PGP");
    }

    /**
     * @return true if this algorithm uses its own internal key derivation process from a password
     */
    public boolean isPBECipher() {
        return algorithm.startsWith("PBE");
    }

    @Override
    public String toString() {
        final ToStringBuilder builder = new ToStringBuilder(this);
        ToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("Algorithm name", algorithm);
        builder.append("Requires unlimited strength JCE policy", unlimitedStrength);
        builder.append("Algorithm Provider", provider);
        builder.append("Compatible with strong KDFs", compatibleWithStrongKDFs);
        builder.append("Keyed cipher", isKeyedCipher());
        return builder.toString();
    }

    public static EncryptionMethod forAlgorithm(String algorithm) {
        if (StringUtils.isNotBlank(algorithm)) {
            for (EncryptionMethod em : EncryptionMethod.values()) {
                if (em.algorithm.equalsIgnoreCase(algorithm)) {
                    return em;
                }
            }
        }
        return null;
    }
}
