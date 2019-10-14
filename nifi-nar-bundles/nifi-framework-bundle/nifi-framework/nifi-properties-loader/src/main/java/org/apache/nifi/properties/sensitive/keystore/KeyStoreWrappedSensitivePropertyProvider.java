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
package org.apache.nifi.properties.sensitive.keystore;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.ExternalProperties;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.StandardExternalPropertyLookup;
import org.apache.nifi.properties.sensitive.aes.AESSensitivePropertyProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Sensitive properties using KeyStore keys with an inner AES SPP.
 */
public class KeyStoreWrappedSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreWrappedSensitivePropertyProvider.class);

    private static final String PROVIDER_NAME = "KeyStore Sensitive Property Provider";
    private static final String MATERIAL_PREFIX = "keystore";
    private static final String MATERIAL_SEPARATOR = "/";

    public static final String KEYSTORE_TYPE_JCECKS = "jceks";
    private static final String KEYSTORE_TYPE_PKCS12 = "pkcs12";
    private static final String KEYSTORE_TYPE_BKS = "bks";

    static final Set<String> KEYSTORE_TYPES = new HashSet<>(Arrays.asList(
            KEYSTORE_TYPE_JCECKS,
            KEYSTORE_TYPE_PKCS12,
            KEYSTORE_TYPE_BKS));

    private final ExternalProperties externalProperties;
    private final SensitivePropertyProvider wrappedSensitivePropertyProvider;
    private final String storeType;
    private final String keyAlias;

    /**
     * Constructor, as expected by the standard sensitive property provider implementation.
     *
     * @param keyId string in the form "keystore/jcecks/user-key-alias"
     */
    public KeyStoreWrappedSensitivePropertyProvider(String keyId)  {
        this(keyId, null, null);
    }

    public KeyStoreWrappedSensitivePropertyProvider(String keyId, KeyStoreProvider keyStoreProvider, ExternalProperties externalProperties)  {
        if (externalProperties == null) {
            externalProperties = new StandardExternalPropertyLookup(null, getKeyStorePropertiesMapping() );
        }
        this.externalProperties = externalProperties;

        if (StringUtils.isBlank(keyId))
            throw new SensitivePropertyConfigurationException("The key cannot be empty");

        String storeType;
        String keyAlias;
        try {
            String[] parts = keyId.split(MATERIAL_SEPARATOR);
            storeType = parts.length > 0 ? parts[1] : "";
            keyAlias = parts.length > 1 ? parts[2] : "";
        } catch (final ArrayIndexOutOfBoundsException e) {
            throw new SensitivePropertyConfigurationException("Invalid Key Store key", e);
        }

        this.storeType = storeType;
        this.keyAlias = keyAlias;

        if (keyStoreProvider == null ){
            keyStoreProvider = new StandardKeyStoreProvider(getStoreUri(), this.storeType, getStorePassword());
        }

        try {
            KeyStore store = keyStoreProvider.getKeyStore();
            Key secretKey = store.getKey(keyAlias, getKeyPassword().toCharArray());
            this.wrappedSensitivePropertyProvider = new AESSensitivePropertyProvider(secretKey.getEncoded());
        } catch (final IOException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
            throw new SensitivePropertyConfigurationException(e);
        }
    }

    private Map<String, String> getKeyStorePropertiesMapping() {
        Map<String, String> map = new HashMap<>();
        map.put("keystore.key-password", "KEYSTORE_KEY_PASSWORD");
        map.put("keystore.password", "KEYSTORE_PASSWORD");
        map.put("keystore.file", "KEYSTORE_FILE");
        return map;
    }

    private String getKeyPassword() {
        return externalProperties.get("keystore.key-password", "");
    }

    private String getStorePassword() {
        return externalProperties.get("keystore.password", "");
    }

    private String getStoreUri() {
        return externalProperties.get("keystore.file", "");
    }

    public static String formatForType(String storeType, String keyAlias) {
        return MATERIAL_PREFIX + MATERIAL_SEPARATOR + storeType + MATERIAL_SEPARATOR + keyAlias;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return PROVIDER_NAME;
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return MATERIAL_PREFIX + MATERIAL_SEPARATOR + storeType + MATERIAL_SEPARATOR + keyAlias;
    }

    /**
     * Returns the "protected" form of this value. This is a form which can safely be persisted in the {@code nifi.properties} file without compromising the value.
     * An encryption-based provider would return a cipher text, while a remote-lookup provider could return a unique ID to retrieve the secured value.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        return wrappedSensitivePropertyProvider.protect(unprotectedValue);
    }

    /**
     * Returns the "unprotected" form of this value. This is the raw sensitive value which is used by the application logic.
     * An encryption-based provider would decrypt a cipher text and return the plaintext, while a remote-lookup provider could retrieve the secured value.
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     */
    @Override
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        try {
            return wrappedSensitivePropertyProvider.unprotect(protectedValue);
        } catch (final IllegalArgumentException e) {
            throw new SensitivePropertyProtectionException(e);
        }
    }

    /**
     * True when the client specifies a key like 'keystore/pkcs12/...'.
     *
     * @param material name of encryption or protection scheme
     * @return true if this class can provide protected values
     */
    public static boolean isProviderFor(String material) {
        if (StringUtils.isBlank(material)) {
            return false;
        }
        String[] parts = material.split(MATERIAL_SEPARATOR, 3);
        return parts.length == 3 && parts[0].toLowerCase().equals(MATERIAL_PREFIX) && KEYSTORE_TYPES.contains(parts[1].toLowerCase());
    }

    /**
     * Returns a printable representation of a key.
     *
     * @param key key material or key id
     * @return printable string
     */
    public static String toPrintableString(String key) {
        return key;
    }
}
