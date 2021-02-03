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
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.apache.nifi.security.util.crypto.KeyedCipherProvider;
import org.apache.nifi.security.util.crypto.PBECipherProvider;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.Objects;

/**
 * Property Encryptor Factory for encapsulating instantiation of Property Encryptors based on various parameters
 */
public class PropertyEncryptorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyEncryptorFactory.class);

    private static final PropertySecretKeyProvider SECRET_KEY_PROVIDER = new StandardPropertySecretKeyProvider();

    private static final String DEFAULT_PASSWORD = "nififtw!";

    private static final String NOTIFICATION_BORDER = "*";

    private static final int NOTIFICATION_WIDTH = 80;

    private static final String NOTIFICATION_DELIMITER = StringUtils.repeat(NOTIFICATION_BORDER, NOTIFICATION_WIDTH);

    private static final String NOTIFICATION = StringUtils.joinWith(System.lineSeparator(),
            System.lineSeparator(),
            NOTIFICATION_DELIMITER,
            StringUtils.center(String.format("FOUND BLANK SENSITIVE PROPERTIES KEY [%s]", NiFiProperties.SENSITIVE_PROPS_KEY), NOTIFICATION_WIDTH),
            StringUtils.center("USING DEFAULT KEY FOR ENCRYPTION", NOTIFICATION_WIDTH),
            StringUtils.center(String.format("SET [%s] TO SECURE SENSITIVE PROPERTIES", NiFiProperties.SENSITIVE_PROPS_KEY), NOTIFICATION_WIDTH),
            NOTIFICATION_DELIMITER
    );

    /**
     * Get Property Encryptor using NiFi Properties
     *
     * @param properties NiFi Properties
     * @return Property Encryptor
     */
    @SuppressWarnings("deprecation")
    public static PropertyEncryptor getPropertyEncryptor(final NiFiProperties properties) {
        Objects.requireNonNull(properties, "NiFi Properties is required");
        final String algorithm = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM);
        String password = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);

        if (StringUtils.isBlank(password)) {
            LOGGER.error(NOTIFICATION);
            password = DEFAULT_PASSWORD;
        }

        final PropertyEncryptionMethod propertyEncryptionMethod = findPropertyEncryptionAlgorithm(algorithm);
        if (propertyEncryptionMethod == null) {
            final EncryptionMethod encryptionMethod = findEncryptionMethod(algorithm);
            if (encryptionMethod.isPBECipher()) {
                final PBECipherProvider cipherProvider = new org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider();
                return new PasswordBasedCipherPropertyEncryptor(cipherProvider, encryptionMethod, password);
            } else {
                final String message = String.format("Algorithm [%s] not supported for Sensitive Properties", encryptionMethod.getAlgorithm());
                throw new UnsupportedOperationException(message);
            }
        } else {
            final KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
            final SecretKey secretKey = SECRET_KEY_PROVIDER.getSecretKey(propertyEncryptionMethod, password);
            final EncryptionMethod encryptionMethod = propertyEncryptionMethod.getEncryptionMethod();
            return new KeyedCipherPropertyEncryptor(keyedCipherProvider, encryptionMethod, secretKey);
        }
    }

    private static PropertyEncryptionMethod findPropertyEncryptionAlgorithm(final String algorithm) {
        PropertyEncryptionMethod foundPropertyEncryptionMethod = null;

        for (final PropertyEncryptionMethod propertyEncryptionMethod : PropertyEncryptionMethod.values()) {
            if (propertyEncryptionMethod.toString().equals(algorithm)) {
                foundPropertyEncryptionMethod = propertyEncryptionMethod;
                break;
            }
        }

        return foundPropertyEncryptionMethod;
    }

    private static EncryptionMethod findEncryptionMethod(final String algorithm) {
        final EncryptionMethod encryptionMethod = EncryptionMethod.forAlgorithm(algorithm);
        if (encryptionMethod == null) {
            final String message = String.format("Encryption Method not found for Algorithm [%s]", algorithm);
            throw new IllegalArgumentException(message);
        }
        return encryptionMethod;
    }
}
