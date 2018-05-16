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
package org.apache.nifi.toolkit.encryptconfig.util

import org.apache.nifi.properties.SensitivePropertyProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

class NiFiRegistryPropertiesEncryptor extends PropertiesEncryptor {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryPropertiesEncryptor.class)

    // TODO, if and when we add a dependency on NiFi Registry, we can import these dependencies array rather than redefining them

    // Defined in nifi-registry-properties: org.apache.nifi.registry.properties.NiFiRegistryProperties
    private static final String SECURITY_KEYSTORE_PASSWD = "nifi.registry.security.keystorePasswd"
    private static final String SECURITY_KEY_PASSWD = "nifi.registry.security.keyPasswd"
    private static final String SECURITY_TRUSTSTORE_PASSWD = "nifi.registry.security.truststorePasswd"

    // Defined in nifi-registry-properties: org.apache.nifi.registry.properties.ProtectedNiFiRegistryProperties
    private static final String ADDITIONAL_SENSITIVE_PROPERTIES_KEY = "nifi.registry.sensitive.props.additional.keys"
    private static final String[] DEFAULT_SENSITIVE_PROPERTIES = [
            SECURITY_KEYSTORE_PASSWD,
            SECURITY_KEY_PASSWD,
            SECURITY_TRUSTSTORE_PASSWD
    ]

    NiFiRegistryPropertiesEncryptor(SensitivePropertyProvider encryptionProvider, SensitivePropertyProvider decryptionProvider) {
        super(encryptionProvider, decryptionProvider)
    }

    @Override
    Properties encrypt(Properties properties) {
        Set<String> propertiesToEncrypt = new HashSet<>()
        propertiesToEncrypt.addAll(DEFAULT_SENSITIVE_PROPERTIES)
        propertiesToEncrypt.addAll(getAdditionalSensitivePropertyKeys(properties))

        return encrypt(properties, propertiesToEncrypt)
    }

    private static String[] getAdditionalSensitivePropertyKeys(Properties properties) {
        String rawAdditionalSensitivePropertyKeys = properties.getProperty(ADDITIONAL_SENSITIVE_PROPERTIES_KEY)
        if (!rawAdditionalSensitivePropertyKeys) {
            return []
        }
        return rawAdditionalSensitivePropertyKeys.split(Pattern.quote(","))
    }

}
