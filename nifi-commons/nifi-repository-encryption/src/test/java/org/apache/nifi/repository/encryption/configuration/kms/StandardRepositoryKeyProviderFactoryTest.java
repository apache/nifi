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
package org.apache.nifi.repository.encryption.configuration.kms;

import org.apache.nifi.repository.encryption.configuration.EncryptedRepositoryType;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.kms.KeyStoreKeyProvider;
import org.apache.nifi.security.kms.StaticKeyProvider;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardRepositoryKeyProviderFactoryTest {
    private static final String KEY = "1234567812345678";

    private static final String KEY_ID = "1";

    private static final char[] PASSWORD = UUID.randomUUID().toString().toCharArray();

    private static final String KEY_STORE_TYPE = "PKCS12";

    private static final String TYPE_EXTENSION = ".p12";

    private static final SecretKey SECRET_KEY = new SecretKeySpec(KEY.getBytes(StandardCharsets.UTF_8), "AES");

    private static final String KEY_PROTECTION_ALGORITHM = "PBEWithHmacSHA256AndAES_256";

    private RepositoryKeyProviderFactory factory;

    @BeforeEach
    public void setProvider() {
        factory = new StandardRepositoryKeyProviderFactory();
    }

    @Test
    public void testGetKeyProviderPropertyNotConfiguredException() {
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null);
        final EncryptedRepositoryType encryptedRepositoryType = EncryptedRepositoryType.CONTENT;

        final EncryptedConfigurationException exception = assertThrows(EncryptedConfigurationException.class, () ->
                factory.getKeyProvider(encryptedRepositoryType, niFiProperties));
        assertTrue(exception.getMessage().contains(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS));
    }

    @Test
    public void testGetKeyProviderNotFound() {
        final String notFoundProvider = "OTHER";
        final Map<String, String> properties = Collections.singletonMap(NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER, notFoundProvider);
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);
        final EncryptedRepositoryType encryptedRepositoryType = EncryptedRepositoryType.CONTENT;

        final EncryptedConfigurationException exception = assertThrows(EncryptedConfigurationException.class, () ->
                factory.getKeyProvider(encryptedRepositoryType, niFiProperties));
        assertTrue(exception.getMessage().contains(notFoundProvider));
    }

    @Test
    public void testGetKeyProviderContentKeyStoreKeyProvider() throws IOException, GeneralSecurityException {
        final Map<String, String> properties = new HashMap<>();

        properties.put(NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER, EncryptionKeyProvider.KEYSTORE.toString());
        properties.put(NiFiProperties.REPOSITORY_ENCRYPTION_KEY_ID, KEY_ID);
        properties.put(NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_PASSWORD, new String(PASSWORD));

        final File keyStoreFile = File.createTempFile(getClass().getSimpleName(), TYPE_EXTENSION);
        keyStoreFile.deleteOnExit();
        try (final FileOutputStream fileOutputStream = new FileOutputStream(keyStoreFile)) {
            setKeyStore(fileOutputStream);
        }

        properties.put(NiFiProperties.REPOSITORY_ENCRYPTION_KEY_PROVIDER_KEYSTORE_LOCATION, keyStoreFile.getAbsolutePath());

        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);
        assertKeyProviderConfigured(KeyStoreKeyProvider.class, EncryptedRepositoryType.CONTENT, niFiProperties);
    }

    @Test
    public void testGetKeyProviderContentStaticKeyProvider() {
        final Map<String, String> properties = new HashMap<>();

        final Class<?> providerClass = StaticKeyProvider.class;
        properties.put(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS, providerClass.getName());
        properties.put(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY, KEY);
        properties.put(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_ID, KEY_ID);

        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);
        assertKeyProviderConfigured(providerClass, EncryptedRepositoryType.CONTENT, niFiProperties);
    }

    @Test
    public void testGetKeyProviderFlowFileStaticKeyProvider() {
        final Map<String, String> properties = new HashMap<>();

        final Class<?> providerClass = StaticKeyProvider.class;
        properties.put(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS, providerClass.getName());
        properties.put(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY, KEY);
        properties.put(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID, KEY_ID);

        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);
        assertKeyProviderConfigured(providerClass, EncryptedRepositoryType.FLOWFILE, niFiProperties);
    }

    @Test
    public void testGetKeyProviderProvenanceStaticKeyProvider() {
        final Map<String, String> properties = new HashMap<>();

        final Class<?> providerClass = StaticKeyProvider.class;
        properties.put(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS, providerClass.getName());
        properties.put(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY, KEY);
        properties.put(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_ID, KEY_ID);

        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);
        assertKeyProviderConfigured(providerClass, EncryptedRepositoryType.PROVENANCE, niFiProperties);
    }

    private void assertKeyProviderConfigured(final Class<?> providerClass, final EncryptedRepositoryType encryptedRepositoryType, final NiFiProperties niFiProperties) {
        final KeyProvider keyProvider = factory.getKeyProvider(encryptedRepositoryType, niFiProperties);
        assertNotNull(keyProvider);
        assertEquals(providerClass, keyProvider.getClass());
        assertTrue(keyProvider.keyExists(KEY_ID));
    }

    private void setKeyStore(final OutputStream outputStream) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(KEY_STORE_TYPE);
        keyStore.load(null, null);
        keyStore.setEntry(KEY_ID, new KeyStore.SecretKeyEntry(SECRET_KEY), new KeyStore.PasswordProtection(PASSWORD, KEY_PROTECTION_ALGORITHM, null));
        keyStore.store(outputStream, PASSWORD);
    }
}
