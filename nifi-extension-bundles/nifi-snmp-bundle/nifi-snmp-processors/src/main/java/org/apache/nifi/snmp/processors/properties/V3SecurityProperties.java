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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.snmp4j.security.SecurityLevel;

import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_V3;
import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_VERSION;

public class V3SecurityProperties {

    private V3SecurityProperties() {
        // Utility class, not needed to instantiate.
    }

    private static final String SHA_2_ALGORITHM = "Provides authentication based on the HMAC-SHA-2 algorithm.";
    private static final String AES_DESCRIPTION = "AES is a symmetric algorithm which uses the same 128, 192, or 256 bit" +
            " key for both encryption and decryption (the security of an AES system increases exponentially with key length).";

    // SNMPv3 security levels
    public static final AllowableValue NO_AUTH_NO_PRIV = new AllowableValue(SecurityLevel.noAuthNoPriv.name(), SecurityLevel.noAuthNoPriv.name(),
            "Communication without authentication and privacy.");
    public static final AllowableValue AUTH_NO_PRIV = new AllowableValue(SecurityLevel.authNoPriv.name(), SecurityLevel.authNoPriv.name(),
            "Communication with authentication and without privacy.");
    public static final AllowableValue AUTH_PRIV = new AllowableValue(SecurityLevel.authPriv.name(), SecurityLevel.authPriv.name(),
            "Communication with authentication and privacy.");

    // SNMPv3 authentication protocols
    public static final AllowableValue HMAC128SHA224 = new AllowableValue("HMAC128SHA224", "SHA224",
            SHA_2_ALGORITHM);
    public static final AllowableValue HMAC192SHA256 = new AllowableValue("HMAC192SHA256", "SHA256",
            SHA_2_ALGORITHM);
    public static final AllowableValue HMAC256SHA384 = new AllowableValue("HMAC256SHA384", "SHA384",
            SHA_2_ALGORITHM);
    public static final AllowableValue HMAC384SHA512 = new AllowableValue("HMAC384SHA512", "SHA512",
            SHA_2_ALGORITHM);

    // SNMPv3 encryption
    public static final AllowableValue DES = new AllowableValue("DES", "DES",
            "Symmetric-key algorithm for the encryption of digital data. DES has been considered insecure" +
                    " because of the feasibility of brute-force attacks. We recommend using the AES encryption protocol.");

    public static final AllowableValue AES128 = new AllowableValue("AES128", "AES128", AES_DESCRIPTION);
    public static final AllowableValue AES192 = new AllowableValue("AES192", "AES192", AES_DESCRIPTION);
    public static final AllowableValue AES256 = new AllowableValue("AES256", "AES256", AES_DESCRIPTION);

    public static final PropertyDescriptor SNMP_SECURITY_LEVEL = new PropertyDescriptor.Builder()
            .name("snmp-security-level")
            .displayName("SNMP Security Level")
            .description("SNMP version 3 provides extra security with User Based Security Model (USM). The three levels of security is " +
                    "1. Communication without authentication and encryption (NoAuthNoPriv). " +
                    "2. Communication with authentication and without encryption (AuthNoPriv). " +
                    "3. Communication with authentication and encryption (AuthPriv).")
            .required(true)
            .allowableValues(NO_AUTH_NO_PRIV, AUTH_NO_PRIV, AUTH_PRIV)
            .defaultValue(NO_AUTH_NO_PRIV.getValue())
            .dependsOn(SNMP_VERSION, SNMP_V3)
            .build();

    public static final PropertyDescriptor SNMP_SECURITY_NAME = new PropertyDescriptor.Builder()
            .name("snmp-security-name")
            .displayName("SNMP Security Name")
            .description("User name used for SNMP v3 Authentication.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V3)
            .build();

    public static final PropertyDescriptor SNMP_AUTH_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-authentication-protocol")
            .displayName("SNMP Authentication Protocol")
            .description("Hash based authentication protocol for secure authentication.")
            .required(true)
            .allowableValues(HMAC128SHA224, HMAC192SHA256, HMAC256SHA384, HMAC384SHA512)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_NO_PRIV, AUTH_PRIV)
            .build();

    public static final PropertyDescriptor SNMP_AUTH_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-authentication-passphrase")
            .displayName("SNMP Authentication Passphrase")
            .description("Passphrase used for SNMP authentication protocol.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_NO_PRIV, AUTH_PRIV)
            .build();

    public static final PropertyDescriptor SNMP_PRIVACY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol")
            .displayName("SNMP Privacy Protocol")
            .description("Privacy allows for encryption of SNMP v3 messages to ensure confidentiality of data.")
            .required(true)
            .allowableValues(DES, AES128, AES192, AES256)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_PRIV)
            .build();

    public static final PropertyDescriptor SNMP_PRIVACY_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol-passphrase")
            .displayName("SNMP Privacy Passphrase")
            .description("Passphrase used for SNMP privacy protocol.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_PRIV)
            .build();
}
