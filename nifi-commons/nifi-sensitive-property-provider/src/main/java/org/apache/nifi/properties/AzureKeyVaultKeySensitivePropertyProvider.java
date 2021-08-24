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
package org.apache.nifi.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.BootstrapProperties.BootstrapPropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.DecryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.security.keyvault.keys.models.KeyProperties;

import java.util.Base64;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

public class AzureKeyVaultKeySensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(AzureKeyVaultKeySensitivePropertyProvider.class);

    private static final String AZURE_PREFIX = "azure";
    private static final String KEYVAULT_KEY_PROPS_NAME = "azure.keyvault.key.id";
    private static final String ENCRYPTION_ALGORITHM_PROPS_NAME = "azure.keyvault.encryption.algorithm";

    private static final Charset PROPERTY_CHARSET = StandardCharsets.UTF_8;

    private final BootstrapProperties azureBootstrapProperties;
    private CryptographyClient client;
    private String keyId;
    private String algorithm;

    AzureKeyVaultKeySensitivePropertyProvider(final BootstrapProperties bootstrapProperties) throws SensitivePropertyProtectionException {
        super(bootstrapProperties);
        Objects.requireNonNull(bootstrapProperties, "Bootstrap Properties required");
        azureBootstrapProperties = getAzureBootstrapProperties(bootstrapProperties);
        loadRequiredAzureProperties(azureBootstrapProperties);
    }

    /**
     * Initializes the Azure Key Vault Cryptography Client to be used for encrypt, decrypt and other interactions with Azure Key Vault.
     * Uses the default Azure credentials provider chain.
     */
    private void initializeClient() {
        if (azureBootstrapProperties == null) {
            logger.warn("Azure Bootstrap Properties are required for Key Vault Client initialization");
            return;
        }

        if (StringUtils.isBlank(keyId)) {
            logger.warn("Cannot initialize client if Azure Key Vault Key ID is blank");
            return;
        }

        try {
            client = new CryptographyClientBuilder()
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .keyIdentifier(keyId)
                    .buildClient();
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException("Azure Key Vault Client initialization failed", e);
        }
    }

    /**
     * Validates the key provided by the user.
     * Note: This function performs checks on the key and indirectly also validates the credentials provided
     * during the initialization of the client.
     */
    private void validate() throws SensitivePropertyProtectionException {
        if (client == null) {
            throw new SensitivePropertyProtectionException("Azure Key Vault validation failed: Client not initialized");
        }

        if (StringUtils.isBlank(keyId)) {
            throw new SensitivePropertyProtectionException("Azure Key Vault validation failed: Key not specified");
        }

        try {
            final KeyProperties keyProps = client.getKey().getProperties();
            if (!keyProps.isEnabled()) {
                throw new SensitivePropertyProtectionException("Azure Key Vault validation failed: Key not enabled");
            }

            final List<KeyOperation> keyOps = client.getKey().getKeyOperations();
            if (!(keyOps.contains(KeyOperation.ENCRYPT) && keyOps.contains(KeyOperation.DECRYPT))) {
                throw new SensitivePropertyProtectionException("Azure Key Vault validation failed: Encrypt and Decrypt not supported");
            }
        } catch (final ResourceNotFoundException e) {
            throw new SensitivePropertyProtectionException("Azure Key Vault validation failed: Key not found", e);
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException("Azure Key Vault validation failed", e);
        }
    }

    /**
     * Checks if we have the required properties {@link #keyId} and {@link #algorithm} from bootstrap-azure.conf
     * for Azure KeyVault and loads it into the appropriate variables, will load null if values don't exist.
     * Note: This function does not verify if the properties are valid.
     * @param props the properties representing bootstrap-azure.conf
     */
    private void loadRequiredAzureProperties(final BootstrapProperties props) {
        if (props != null) {
            keyId = props.getProperty(KEYVAULT_KEY_PROPS_NAME);
            algorithm = props.getProperty(ENCRYPTION_ALGORITHM_PROPS_NAME);
        }
    }


    /**
     * Checks bootstrap.conf to check if BootstrapPropertyKey.AZURE_KEYVAULT_SENSITIVE_PROPERTY_PROVIDER_CONF property is configured to the
     * bootstrap-azure.conf file. Also will load bootstrap-azure.conf to {@link #azureBootstrapProperties} if possible
     * @param bootstrapProperties BootstrapProperties object corresponding to bootstrap.conf
     * @return BootstrapProperties object corresponding to bootstrap-azure.conf, null otherwise
     */
    private BootstrapProperties getAzureBootstrapProperties(final BootstrapProperties bootstrapProperties) {
        final BootstrapProperties cloudBootstrapProperties;

        // Load the bootstrap-azure.conf file based on path specified in
        // "nifi.bootstrap.protection.azure.keyvault.conf" property of bootstrap.conf
        final String filePath = bootstrapProperties.getProperty(BootstrapPropertyKey.AZURE_KEYVAULT_SENSITIVE_PROPERTY_PROVIDER_CONF).orElse(null);
        if (StringUtils.isBlank(filePath)) {
            logger.warn("Azure Key Vault properties file path not configured in bootstrap properties");
            return null;
        }

        try {
            cloudBootstrapProperties = AbstractBootstrapPropertiesLoader.loadBootstrapProperties(
                    Paths.get(filePath), AZURE_PREFIX);
        } catch (final IOException e) {
            throw new SensitivePropertyProtectionException("Could not load " + filePath, e);
        }

        return cloudBootstrapProperties;
    }

    /**
     * Checks the BootstrapProperties corresponding to bootstrap-azure.conf for the required configurations
     * for Azure encrypt/decrypt operations.
     * Note: This does not check for credentials/region configurations.
     * Credentials/configuration will be checked during the first protect/unprotect call during runtime.
     * @return True if bootstrap-azure.conf contains the required properties for Azure SPP, False otherwise
     */
    private boolean hasRequiredAzureProperties() {
        return azureBootstrapProperties != null && StringUtils.isNoneBlank(keyId, algorithm);
    }

    /**
     * Return true if this SensitivePropertyProvider is supported, given the provided Bootstrap properties.
     * @return True if this SensitivePropertyProvider is supported
     */
    @Override
    public boolean isSupported() {
        return hasRequiredAzureProperties();
    }

    /**
     * Return the appropriate PropertyProtectionScheme for this provider.
     *
     * @return The PropertyProtectionScheme
     */
    @Override
    protected PropertyProtectionScheme getProtectionScheme() {
        return PropertyProtectionScheme.AZURE_KEYVAULT_KEY;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return PropertyProtectionScheme.AZURE_KEYVAULT_KEY.getName();
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return PropertyProtectionScheme.AZURE_KEYVAULT_KEY.getIdentifier();
    }


    /**
     * Returns the ciphertext of this value encrypted using a key stored in Azure Key Vault.
     *
     * @return the ciphertext blob to persist in the {@code nifi.properties} file
     */
    private byte[] encrypt(final byte[] input) {
        EncryptResult encryptResult = client.encrypt(EncryptionAlgorithm.fromString(algorithm), input);
        return encryptResult.getCipherText();
    }

    /**
     * Returns the value corresponding to a ciphertext decrypted using a key stored in Azure Key Vault
     *
     * @return the "unprotected" byte[] of this value, which could be used by the application
     */
    private byte[] decrypt(final byte[] input) {
        DecryptResult decryptResult = client.decrypt(EncryptionAlgorithm.fromString(algorithm), input);
        return decryptResult.getPlainText();
    }

    /**
     * Checks if the client is open and if not, initializes the client and validates the configuration required for Azure Key Vault.
     */
    private void checkAndInitializeClient() {
        if (client == null) {
            initializeClient();
            validate();
        }
    }

    /**
     * Returns the "protected" form of this value. This is a form which can safely be persisted in the {@code nifi.properties} file without compromising the value.
     * Encrypts a sensitive value using a key managed by Azure Key Vault.
     *
     * @param unprotectedValue the sensitive value
     * @param context The context of the value (ignored in this implementation)
     * @return the value to persist in the {@code nifi.properties} file
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt a blank value");
        }

        checkAndInitializeClient();

        try {
            final byte[] plainBytes = unprotectedValue.getBytes(PROPERTY_CHARSET);
            final byte[] cipherBytes = encrypt(plainBytes);
            return Base64.getEncoder().encodeToString(cipherBytes);
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException("Encrypt failed", e);
        }
    }

    /**
     * Returns the "unprotected" form of this value. This is the raw sensitive value which is used by the application logic.
     * Decrypts a secured value from a ciphertext using a key managed by Azure Key Vault.
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file
     * @param context The context of the value (ignored in this implementation)
     * @return the raw value to be used by the application
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(protectedValue)) {
            throw new IllegalArgumentException("Cannot decrypt a blank value");
        }

        checkAndInitializeClient();

        try {
            final byte[] cipherBytes = Base64.getDecoder().decode(protectedValue);
            final byte[] plainBytes = decrypt(cipherBytes);
            return new String(plainBytes, PROPERTY_CHARSET);
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException("Decrypt failed", e);
        }
    }

    /**
     * Nothing required to be done for Azure Client cleanUp function.
     */
    @Override
    public void cleanUp() {}
}
