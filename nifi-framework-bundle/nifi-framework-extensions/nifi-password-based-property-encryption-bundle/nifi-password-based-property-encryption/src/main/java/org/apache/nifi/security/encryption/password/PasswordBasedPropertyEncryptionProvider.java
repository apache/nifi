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
package org.apache.nifi.security.encryption.password;

import org.apache.nifi.encrypt.PropertyEncryptionMethod;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.security.encryption.PropertyEncryptionException;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.PropertyEncryptionProviderInitializationContext;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.apache.nifi.util.NiFiProperties;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Property Encryption Provider that derives an AES-GCM secret key from the sensitive properties password configured in
 * application properties. Encrypted values consist of a random initialization vector followed by cipher text, matching
 * the binary representation of sensitive values protected using the sensitive properties key and algorithm.
 */
public class PasswordBasedPropertyEncryptionProvider implements PropertyEncryptionProvider {
    private static final String DEFAULT_ALGORITHM = PropertyEncryptionMethod.NIFI_PBKDF2_AES_GCM_256.toString();

    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private static final String PASSWORD_REQUIRED = String.format("Sensitive Properties Key [%s] required", NiFiProperties.SENSITIVE_PROPS_KEY);

    private final NiFiProperties properties;

    private volatile PropertyEncryptor propertyEncryptor;

    public PasswordBasedPropertyEncryptionProvider(final NiFiProperties properties) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
    }

    /**
     * Derive the secret key from the configured sensitive properties password and algorithm
     *
     * @param context Initialization context containing configured properties
     */
    @Override
    public void initialize(final PropertyEncryptionProviderInitializationContext context) {
        final String password = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);
        if (password == null || password.isBlank()) {
            throw new PropertyEncryptionException(PASSWORD_REQUIRED);
        }

        final PropertyEncryptorBuilder builder = new PropertyEncryptorBuilder(password);
        final String algorithm = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, DEFAULT_ALGORITHM);
        builder.setAlgorithm(algorithm);

        try {
            propertyEncryptor = builder.build();
        } catch (final RuntimeException e) {
            throw new PropertyEncryptionException("Secret Key derivation failed", e);
        }
    }

    @Override
    public byte[] encrypt(final byte[] property, final SensitivePropertyContext context) {
        Objects.requireNonNull(property, "Property required");
        final String propertyValue = new String(property, StandardCharsets.UTF_8);

        try {
            final String encrypted = propertyEncryptor.encrypt(propertyValue);
            return HEX_FORMAT.parseHex(encrypted);
        } catch (final RuntimeException e) {
            throw new PropertyEncryptionException("Property encryption failed", e);
        }
    }

    @Override
    public byte[] decrypt(final byte[] encryptedProperty, final SensitivePropertyContext context) {
        Objects.requireNonNull(encryptedProperty, "Encrypted Property required");
        final String encrypted = HEX_FORMAT.formatHex(encryptedProperty);

        try {
            final String decrypted = propertyEncryptor.decrypt(encrypted);
            return decrypted.getBytes(StandardCharsets.UTF_8);
        } catch (final RuntimeException e) {
            throw new PropertyEncryptionException("Property decryption failed", e);
        }
    }
}
