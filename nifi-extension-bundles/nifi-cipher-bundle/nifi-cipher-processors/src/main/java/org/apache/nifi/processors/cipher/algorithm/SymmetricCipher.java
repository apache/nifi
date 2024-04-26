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
package org.apache.nifi.processors.cipher.algorithm;

import org.apache.nifi.components.DescribedValue;

/**
 * Symmetric Cipher Algorithm Names as enumerated in Java Cryptography Architecture Standard Algorithm Name Documentation
 */
public enum SymmetricCipher implements DescribedValue {
    AES("AES", "Advanced Encryption Standard defined in FIPS 197"),
    DES("DES", "Data Encryption Standard defined in FIPS 46-3 and withdrawn in 2005"),
    DESEDE("DESede", "Triple Data Encryption Standard also known as 3DES and deprecated in 2023"),
    RC2("RC2", "RSA Rivest Cipher 2 defined in RFC 2268"),
    RC4("RC4", "RSA Rivest Cipher 4"),
    TWOFISH("TWOFISH", "Twofish Block Cipher");

    private final String name;
    private final String description;

    SymmetricCipher(final String name, final String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name;
    }

    @Override
    public String getDisplayName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
