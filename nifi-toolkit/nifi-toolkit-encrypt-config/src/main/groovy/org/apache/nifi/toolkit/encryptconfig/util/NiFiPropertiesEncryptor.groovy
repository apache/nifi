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

import org.apache.nifi.properties.ProtectedNiFiProperties
import org.apache.nifi.properties.SensitivePropertyProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

class NiFiPropertiesEncryptor extends PropertiesEncryptor {

    private static final Logger logger = LoggerFactory.getLogger(NiFiPropertiesEncryptor.class)

    private static final String ADDITIONAL_SENSITIVE_PROPERTIES_KEY = ProtectedNiFiProperties.ADDITIONAL_SENSITIVE_PROPERTIES_KEY
    private static final String[] DEFAULT_SENSITIVE_PROPERTIES = ProtectedNiFiProperties.DEFAULT_SENSITIVE_PROPERTIES

    NiFiPropertiesEncryptor(SensitivePropertyProvider encryptionProvider, SensitivePropertyProvider decryptionProvider) {
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
