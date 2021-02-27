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
package org.apache.nifi.encrypt;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;

import java.util.Objects;

/**
 * Property Encryptor Factory for encapsulating instantiation of Property Encryptors based on various parameters
 */
public class PropertyEncryptorFactory {
    private static final String KEY_REQUIRED = String.format("NiFi Sensitive Properties Key [%s] is required", NiFiProperties.SENSITIVE_PROPS_KEY);

    /**
     * Get Property Encryptor using NiFi Properties
     *
     * @param properties NiFi Properties
     * @return Property Encryptor
     */
    public static PropertyEncryptor getPropertyEncryptor(final NiFiProperties properties) {
        Objects.requireNonNull(properties, "NiFi Properties is required");
        final String algorithm = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM);
        String password = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);

        if (StringUtils.isBlank(password)) {
            throw new IllegalArgumentException(KEY_REQUIRED);
        }

        return new PropertyEncryptorBuilder(password).setAlgorithm(algorithm).build();
    }
}
