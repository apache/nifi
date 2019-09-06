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
package org.apache.nifi.properties.sensitive.hashicorp.vault;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.ExternalProperties;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.StandardExternalPropertyLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.io.Resource;
import org.springframework.vault.authentication.SimpleSessionManager;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.config.ClientHttpRequestFactoryFactory;
import org.springframework.vault.core.VaultOperations;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.SslConfiguration;
import org.springframework.vault.support.ClientOptions;

import java.net.URI;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Sensitive properties using Vault Transit encrypt and decrypt operations.
 */
public class VaultSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(VaultSensitivePropertyProvider.class);

    private static final String PROVIDER_NAME = "HashiCorp Vault Sensitive Property Provider";
    static final String MATERIAL_PREFIX = "vault";
    static final String MATERIAL_SEPARATOR = "/";

    static final String VAULT_AUTH_TOKEN = "token";
    static final String VAULT_AUTH_APP_ID = "appid";
    static final String VAULT_AUTH_APP_ROLE = "approle";
    static final String VAULT_AUTH_CUBBYHOLE = "cubbyhole";

    private static final Set<String> VAULT_AUTH_TYPES = new HashSet<>(Arrays.asList(
            VAULT_AUTH_TOKEN,
            VAULT_AUTH_APP_ID,
            VAULT_AUTH_APP_ROLE,
            VAULT_AUTH_CUBBYHOLE
    ));

    private final VaultOperations vaultOperations;
    private final ExternalProperties externalProperties;
    private final String transitKeyId;
    private final String authType;

    /**
     * Constructs a {@link SensitivePropertyProvider} that uses Vault encrypt and decrypt values.
     *
     * @param keyId vault key spec, in the form "vault/{auth-type}/{transit-key-id}"
     *
     */
    public VaultSensitivePropertyProvider(String keyId) {
        this(keyId, new StandardExternalPropertyLookup(null, getVaultPropertiesMapping()));
    }

    /**
     * Constructs a {@link SensitivePropertyProvider} that uses Vault encrypt and decrypt values.
     *
     * @param keyId vault key spec, in the form "vault/{auth-type}/{transit-key-id}"
     * @param externalProperties External properties provider
     */
    public VaultSensitivePropertyProvider(String keyId, ExternalProperties externalProperties) {
        this.externalProperties = externalProperties;
        transitKeyId = getTransitKey(keyId);
        authType = getVaultAuthentication(keyId);

        String serverUri = getVaultUri();

        if (StringUtils.isBlank(authType) || StringUtils.isBlank(serverUri) || StringUtils.isBlank(transitKeyId))
            throw new SensitivePropertyConfigurationException("The key cannot be empty");

        VaultEndpoint vaultEndpoint = VaultEndpoint.from(URI.create(serverUri));
        StandardVaultConfiguration config = new StandardVaultConfiguration(vaultEndpoint, externalProperties);

        Resource trustStore = config.getVaultSslTrustStore();
        Resource keyStore = config.getVaultSslKeyStore();

        String storeType = KeyStore.getDefaultType();
        SslConfiguration sslConf;


        if (keyStore == null && trustStore == null) {
            sslConf = SslConfiguration.NONE;
        //} else if ((keyStore == null) || (trustStore == null)) {
        //    throw new SensitivePropertyConfigurationException("Vault TLS requires key store and trust store properties");
        } else {
            SslConfiguration.KeyStoreConfiguration keyStoreConf = new SslConfiguration.KeyStoreConfiguration(keyStore, config.getVaultSslKeyStorePassword(), storeType);
            SslConfiguration.KeyStoreConfiguration trustStoreConf = new SslConfiguration.KeyStoreConfiguration(trustStore, config.getVaultSslTrustStorePassword(), storeType);
            sslConf = new SslConfiguration(keyStoreConf, trustStoreConf);
        }

        vaultOperations = new VaultTemplate(vaultEndpoint, ClientHttpRequestFactoryFactory.create(new ClientOptions(), sslConf), new SimpleSessionManager(config.clientAuthentication()));
    }

    /**
     * Extract the Vault URI from the external properties.
     *
     * @return Vault server URI
     */
    private String getVaultUri() {
        return this.externalProperties.get("vault.uri");
    }

    /**
     * Extract the Vault auth method from the external properties.
     *
     * Note that this method does not reference the system property `vault.authentication or a similar environment variable.
     * This is because our auth method is embedded in the key, e.g., vault/appid/some-token.
     *
     * @return auth method
     * @param keyId key identifier
     */
    private String getVaultAuthentication(String keyId) {
        String[] parts = keyId.split(MATERIAL_SEPARATOR);
        return parts.length == 3 && VAULT_AUTH_TYPES.contains(parts[1]) ? parts[1] : "";
    }

    private static String getTransitKey(String keyId) {
        String[] parts = keyId.split(MATERIAL_SEPARATOR, 3);
        return parts.length == 3 && VAULT_AUTH_TYPES.contains(parts[1]) ? parts[2] : "";
    }

    /**
     * Creates a Vault key spec string for token authentication.
     */
    static String formatForTokenAuth(String keyId) {
        return MATERIAL_PREFIX + MATERIAL_SEPARATOR + VAULT_AUTH_TOKEN + MATERIAL_SEPARATOR + keyId;
    }

    /**
     * Creates a Vault key spec string for token authentication.
     */
    static String formatForCubbyholeAuth(String keyId) {
        return MATERIAL_PREFIX + MATERIAL_SEPARATOR + VAULT_AUTH_CUBBYHOLE + MATERIAL_SEPARATOR + keyId;
    }

    /**
     * Creates a Vault key spec string for app role authentication.
     */
    static String formatForAppRoleAuth(String keyId) {
        return MATERIAL_PREFIX + MATERIAL_SEPARATOR + VAULT_AUTH_APP_ROLE + MATERIAL_SEPARATOR + keyId;
    }

    /**
     * Creates a Vault key spec string for app id authentication.
     */
    static String formatForAppIdAuth(String keyId) {
        return MATERIAL_PREFIX + MATERIAL_SEPARATOR + VAULT_AUTH_APP_ID + MATERIAL_SEPARATOR + keyId;
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
        return MATERIAL_PREFIX + MATERIAL_SEPARATOR + authType + MATERIAL_SEPARATOR + transitKeyId;
    }

    /**
     * Returns the encrypted cipher text.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception encrypting the value
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        if (unprotectedValue == null || StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt an empty value");
        }
        String vaultResponse;
        vaultResponse = vaultTransitEncrypt(vaultOperations, transitKeyId, unprotectedValue);
        if (vaultResponse == null) {
            throw new SensitivePropertyProtectionException("Empty response during wrap.");
        }
        return vaultResponse;
    }

    /**
     * Returns the decrypted plaintext.
     *
     * @param protectedValue the cipher text read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     * @throws SensitivePropertyProtectionException if there is an error decrypting the cipher text
     */
    @Override
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        String vaultResponse;
        try {
            vaultResponse = vaultTransitDecrypt(vaultOperations, transitKeyId, protectedValue);
        } catch (final org.springframework.vault.VaultException e) {
            throw new SensitivePropertyProtectionException(e);
        }

        if (vaultResponse == null) {
            throw new SensitivePropertyProtectionException("Empty response during unwrap.");
        }
        return vaultResponse;
    }

    /**
     * True when the client specifies a key like 'vault/token/...'.
     *
     * @param material name of encryption or protection scheme
     * @return true if this class can provide protected values
     */
    public static boolean isProviderFor(String material) {
        if (StringUtils.isBlank(material)) {
            return false;
        }
        String[] parts = material.split(MATERIAL_SEPARATOR, 3);
        return parts.length == 3 && StringUtils.equals(parts[0], MATERIAL_PREFIX) && VAULT_AUTH_TYPES.contains(parts[1]);
    }

    /**
     * Returns a printable representation of this instance.
     *
     * @param key Vault client material
     * @return printable string
     */
    public static String toPrintableString(String key) {
        return key;
    }

    /**
     * Unwraps the given vault (string) token.
     *
     * @param vaultOperations Vault client
     * @param keyId transit key id
     * @param cipherText encrypted text to decrypt
     * @return deciphered text
     */
    static String vaultTransitDecrypt(VaultOperations vaultOperations, String keyId, String cipherText) {
        return vaultOperations.opsForTransit().decrypt(keyId, cipherText);
    }

    /**
     * Wraps the given map.
     *
     * @param vaultOperations Vault client
     * @param keyId transit key id
     * @param plainText plaintext to encrypt
     * @return ciphered text
     */
    static String vaultTransitEncrypt(VaultOperations vaultOperations, String keyId, String plainText) {
        return vaultOperations.opsForTransit().encrypt(keyId, plainText);
    }

    private static Map<String, String> getVaultPropertiesMapping() {
        Map<String, String> map = new HashMap<>();
        map.put("vault.uri", "VAULT_ADDR");
        // map.put("vault.authentication", "VAULT_AUTH"); // not used; see note in `getVaultAuthentication`
        map.put("vault.token", "VAULT_TOKEN");

        map.put("vault.app-role.role-id", "VAULT_ROLE_ID");
        map.put("vault.app-role.secret-id", "VAULT_SECRET_ID");

        map.put("vault.app-id.app-id", "VAULT_APP_ID");
        map.put("vault.app-id.user-id", "VAULT_USER_ID");

        map.put("vault.ssl.trust-store", "VAULT_SSL_TRUST_STORE");
        map.put("vault.ssl.trust-store-password", "VAULT_SSL_TRUST_STORE_PASSWORD");

        map.put("vault.ssl.key-store", "VAULT_SSL_KEY_STORE");
        map.put("vault.ssl.key-store-password", "VAULT_SSL_KEY_STORE_PASSWORD");

        return map;
    }
}
