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

import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.security.encryption.PropertyEncryptionException;
import org.apache.nifi.security.encryption.PropertyEncryptionProviderInitializationContext;
import org.apache.nifi.security.encryption.SensitivePropertyAttribute;
import org.apache.nifi.security.encryption.SensitivePropertyCategory;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PasswordBasedPropertyEncryptionProviderTest {
    /** Empty path disables reading application properties from a file */
    private static final String PROPERTIES_FILE_PATH_DISABLED = "";

    private static final String PASSWORD = "sensitive-properties-password";

    private static final String ALGORITHM = "NIFI_PBKDF2_AES_GCM_256";

    private static final String PROPERTY = "Property Value";

    private static final byte[] PROPERTY_BINARY = PROPERTY.getBytes(StandardCharsets.UTF_8);

    private static final String UNICODE_PROPERTY = "Property Value café 東京";

    private static final byte[] UNICODE_PROPERTY_BINARY = UNICODE_PROPERTY.getBytes(StandardCharsets.UTF_8);

    private static final String UNICODE_PBKDF2_ENCRYPTED =
            "000102030405060708090a0b0c0d0e0f8d220b8ce0944d07a30bfb912087fc564bf73f06da92cd5efe6917eed3404f913236ea843a88a44e699276";

    private static final String DIFFERENT_PASSWORD = "different-sensitive-password";

    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private static final SensitivePropertyContext CONTEXT = new SensitivePropertyContext(
            SensitivePropertyCategory.COMPONENT_PROPERTY,
            Map.of(SensitivePropertyAttribute.PROPERTY_NAME, "Password")
    );

    private static final PropertyEncryptionProviderInitializationContext INITIALIZATION_CONTEXT = Mockito.mock(PropertyEncryptionProviderInitializationContext.class);

    private static PasswordBasedPropertyEncryptionProvider provider;

    @BeforeAll
    static void setProvider() {
        provider = getProvider(PASSWORD, ALGORITHM);
        provider.initialize(INITIALIZATION_CONTEXT);
    }

    @Test
    void testEncryptDecrypt() {
        final byte[] encrypted = provider.encrypt(PROPERTY_BINARY, CONTEXT);
        assertFalse(Arrays.equals(PROPERTY_BINARY, encrypted));

        final byte[] decrypted = provider.decrypt(encrypted, CONTEXT);
        assertArrayEquals(PROPERTY_BINARY, decrypted);
    }

    @Test
    void testEncryptRandomizedPerInvocation() {
        final byte[] firstEncrypted = provider.encrypt(PROPERTY_BINARY, CONTEXT);
        final byte[] secondEncrypted = provider.encrypt(PROPERTY_BINARY, CONTEXT);

        assertFalse(Arrays.equals(firstEncrypted, secondEncrypted));
        assertArrayEquals(PROPERTY_BINARY, provider.decrypt(firstEncrypted, CONTEXT));
        assertArrayEquals(PROPERTY_BINARY, provider.decrypt(secondEncrypted, CONTEXT));
    }

    @Test
    void testDecryptSensitivePropertiesBinary() {
        final byte[] encrypted = HEX_FORMAT.parseHex(UNICODE_PBKDF2_ENCRYPTED);

        final byte[] decrypted = provider.decrypt(encrypted, CONTEXT);

        assertArrayEquals(UNICODE_PROPERTY_BINARY, decrypted);
    }

    @Test
    void testEncryptSensitivePropertiesBinary() {
        final byte[] encrypted = provider.encrypt(PROPERTY_BINARY, CONTEXT);

        final PropertyEncryptor propertyEncryptor = new PropertyEncryptorBuilder(PASSWORD).setAlgorithm(ALGORITHM).build();
        final String decrypted = propertyEncryptor.decrypt(HEX_FORMAT.formatHex(encrypted));

        assertEquals(PROPERTY, decrypted);
    }

    @Test
    void testDecryptBinaryNotValid() {
        assertThrows(PropertyEncryptionException.class, () -> provider.decrypt(PROPERTY_BINARY, CONTEXT));
    }

    @Test
    void testDecryptAuthenticationFailed() {
        final byte[] encrypted = provider.encrypt(PROPERTY_BINARY, CONTEXT);
        encrypted[0] = 0;

        assertThrows(PropertyEncryptionException.class, () -> provider.decrypt(encrypted, CONTEXT));
    }

    @Test
    void testDecryptDifferentPassword() {
        final PasswordBasedPropertyEncryptionProvider differentPasswordProvider = getProvider(DIFFERENT_PASSWORD, ALGORITHM);
        differentPasswordProvider.initialize(INITIALIZATION_CONTEXT);
        final byte[] encrypted = provider.encrypt(PROPERTY_BINARY, CONTEXT);

        assertThrows(PropertyEncryptionException.class, () -> differentPasswordProvider.decrypt(encrypted, CONTEXT));
    }

    @Test
    void testInitializeAlgorithmNotConfigured() {
        final PasswordBasedPropertyEncryptionProvider defaultAlgorithmProvider = getProvider(PASSWORD, null);
        defaultAlgorithmProvider.initialize(INITIALIZATION_CONTEXT);

        final byte[] encrypted = defaultAlgorithmProvider.encrypt(PROPERTY_BINARY, CONTEXT);

        assertArrayEquals(PROPERTY_BINARY, defaultAlgorithmProvider.decrypt(encrypted, CONTEXT));
    }

    @Test
    void testInitializePasswordNotConfigured() {
        final PasswordBasedPropertyEncryptionProvider passwordNotConfiguredProvider = getProvider(null, ALGORITHM);

        assertThrows(PropertyEncryptionException.class, () -> passwordNotConfiguredProvider.initialize(INITIALIZATION_CONTEXT));
    }

    @Test
    void testInitializeAlgorithmNotSupported() {
        final PasswordBasedPropertyEncryptionProvider algorithmNotSupportedProvider = getProvider(PASSWORD, "ALGORITHM_NOT_SUPPORTED");

        assertThrows(PropertyEncryptionException.class, () -> algorithmNotSupportedProvider.initialize(INITIALIZATION_CONTEXT));
    }

    @Test
    void testEncryptNotInitialized() {
        final PasswordBasedPropertyEncryptionProvider notInitializedProvider = getProvider(PASSWORD, ALGORITHM);

        assertThrows(PropertyEncryptionException.class, () -> notInitializedProvider.encrypt(PROPERTY_BINARY, CONTEXT));
    }

    private static PasswordBasedPropertyEncryptionProvider getProvider(final String password, final String algorithm) {
        final Map<String, String> configuredProperties = new HashMap<>();
        if (password != null) {
            configuredProperties.put(NiFiProperties.SENSITIVE_PROPS_KEY, password);
        }
        if (algorithm != null) {
            configuredProperties.put(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, algorithm);
        }

        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(PROPERTIES_FILE_PATH_DISABLED, configuredProperties);
        return new PasswordBasedPropertyEncryptionProvider(properties);
    }
}
