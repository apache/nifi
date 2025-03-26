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
import org.snmp4j.security.AuthHMAC128SHA224;
import org.snmp4j.security.AuthHMAC192SHA256;
import org.snmp4j.security.AuthHMAC256SHA384;
import org.snmp4j.security.AuthHMAC384SHA512;
import org.snmp4j.smi.OID;

public enum AuthenticationProtocol implements DescribedValue {
    HMAC128SHA224("HMAC128SHA224", "SHA224",
            "HMAC with SHA224, a variant of SHA-2, used for ensuring data integrity and authenticity. It combines the HMAC construction with the SHA224 hash function.",
            AuthHMAC128SHA224.ID),
    HMAC192SHA256("HMAC192SHA256", "SHA256",
            "HMAC with SHA256, a widely used secure hash function in the SHA-2 family, providing strong data integrity and authenticity guarantees.",
            AuthHMAC192SHA256.ID),
    HMAC256SHA384("HMAC256SHA384", "SHA384",
            "HMAC with SHA384, a stronger variant of SHA-2 providing a 384-bit hash for increased security in data integrity and authenticity.",
            AuthHMAC256SHA384.ID),
    HMAC384SHA512("HMAC384SHA512", "SHA512",
            "HMAC with SHA512, using the SHA-2 family with a 512-bit hash, providing the highest level of security for data integrity and authenticity.",
            AuthHMAC384SHA512.ID);

    private final String value;
    private final String displayName;
    private final String description;
    private final OID oid;

    AuthenticationProtocol(final String value, final String displayName, final String description, final OID oid) {
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
