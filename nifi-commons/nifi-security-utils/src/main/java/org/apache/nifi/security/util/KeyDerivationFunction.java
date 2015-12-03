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

    NIFI_LEGACY("NiFi legacy KDF", "MD5 @ 1000 iterations"),
    OPENSSL_EVP_BYTES_TO_KEY("OpenSSL EVP_BytesToKey", "Single iteration MD5 compatible with PKCS#5 v1.5");
    // TODO: Implement bcrypt, scrypt, and PBKDF2

    private final String name;
    private final String description;

    KeyDerivationFunction(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        final ToStringBuilder builder = new ToStringBuilder(this);
        ToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("KDF Name", name);
        builder.append("Description", description);
        return builder.toString();
    }
}