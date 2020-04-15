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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Enumeration capturing essential information about the various key derivation functions that might be supported.
 */
public enum KeyDerivationFunction {

    NONE("None", "The cipher is given a raw key conforming to the algorithm specifications"),
    NIFI_LEGACY("NiFi Legacy KDF", "MD5 @ 1000 iterations"),
    OPENSSL_EVP_BYTES_TO_KEY("OpenSSL EVP_BytesToKey", "Single iteration MD5 compatible with PKCS#5 v1.5"),
    BCRYPT("Bcrypt", "Bcrypt with configurable work factor. See Admin Guide"),
    SCRYPT("Scrypt", "Scrypt with configurable cost parameters. See Admin Guide"),
    PBKDF2("PBKDF2", "PBKDF2 with configurable hash function and iteration count. See Admin Guide"),
    ARGON2("Argon2", "Argon2 with configurable cost parameters. See Admin Guide.");

    private final String kdfName;
    private final String description;

    KeyDerivationFunction(String kdfName, String description) {
        this.kdfName = kdfName;
        this.description = description;
    }

    public String getKdfName() {
        return kdfName;
    }

    public String getDescription() {
        return description;
    }

    public boolean isStrongKDF() {
        return (kdfName.equals(BCRYPT.kdfName) || kdfName.equals(SCRYPT.kdfName) || kdfName.equals(PBKDF2.kdfName) || kdfName.equals(ARGON2.kdfName));
    }

    public boolean hasFormattedSalt() {
        return kdfName.equals(BCRYPT.kdfName) || kdfName.equals(SCRYPT.kdfName) || kdfName.equals(ARGON2.kdfName);
    }

    @Override
    public String toString() {
        final ToStringBuilder builder = new ToStringBuilder(this);
        ToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("KDF Name", kdfName);
        builder.append("Description", description);
        return builder.toString();
    }
}