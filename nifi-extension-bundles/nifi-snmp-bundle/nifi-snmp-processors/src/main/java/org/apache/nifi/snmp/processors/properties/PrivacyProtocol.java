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
package org.apache.nifi.snmp.processors.properties;

import org.apache.nifi.components.DescribedValue;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivAES192;
import org.snmp4j.security.PrivAES256;
import org.snmp4j.security.PrivDES;
import org.snmp4j.smi.OID;

public enum PrivacyProtocol implements DescribedValue {
    DES("DES", "DES",
            "Data Encryption Standard (DES) is an older symmetric-key algorithm used for encrypting digital data. DES is considered insecure due" +
                    " to its short 56-bit key length, which is vulnerable to brute-force attacks. It is now generally replaced by stronger encryption protocols.",
            PrivDES.ID),
    AES128("AES128", "AES128",
            "Advanced Encryption Standard (AES) with a 128-bit key length, offering a balance between speed and security. AES128 is widely considered" +
                    " to be secure and is commonly used in various cryptographic applications.",
            PrivAES128.ID),
    AES192("AES192", "AES192",
            "Advanced Encryption Standard (AES) with a 192-bit key length, providing a higher level of security than AES128, suitable for applications" +
                    " that require stronger encryption.",
            PrivAES192.ID),
    AES256("AES256", "AES256",
            "Advanced Encryption Standard (AES) with a 256-bit key length, offering the highest level of encryption strength. AES256 is the" +
                    " recommended choice for high-security applications and is considered highly resistant to brute-force attacks.",
            PrivAES256.ID);

    private final String value;
    private final String displayName;
    private final String description;
    private final OID oid;

    PrivacyProtocol(final String value, final String displayName, final String description, final OID oid) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
        this.oid = oid;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public OID getOid() {
        return oid;
    }
}
