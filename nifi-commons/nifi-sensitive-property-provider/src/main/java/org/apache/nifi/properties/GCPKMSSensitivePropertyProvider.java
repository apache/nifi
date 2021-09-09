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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Objects;

public class GCPKMSSensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(GCPKMSSensitivePropertyProvider.class);

    private static final String GCP_PREFIX = "gcp";
    private static final String PROJECT_ID_PROPS_NAME = "gcp.kms.project";
    private static final String LOCATION_ID_PROPS_NAME = "gcp.kms.location";
    private static final String KEYRING_ID_PROPS_NAME = "gcp.kms.keyring";
    private static final String KEY_ID_PROPS_NAME = "gcp.kms.key";

    private static final Charset PROPERTY_CHARSET = StandardCharsets.UTF_8;

    private final BootstrapProperties gcpBootstrapProperties;
    private KeyManagementServiceClient client;
    private CryptoKeyName keyName;

    GCPKMSSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) {
        super(bootstrapProperties);
        Objects.requireNonNull(bootstrapProperties, "The file bootstrap.conf provided to GCP SPP is null");
        gcpBootstrapProperties = getGCPBootstrapProperties(bootstrapProperties);
        loadRequiredGCPProperties(gcpBootstrapProperties);
    }

    /**
     * Initializes the GCP KMS Client to be used for encrypt, decrypt and other interactions with GCP Cloud KMS.
     * Note: This does not verify if credentials are valid.
     */
    private void initializeClient() {
        try {
            client = KeyManagementServiceClient.create();
        } catch (final IOException e) {
            final String msg = "Encountered an error initializing GCP Cloud KMS client";
            throw new SensitivePropertyProtectionException(msg, e);
        }
    }

    /**
     * Validates the key details provided by the user.
     */
    private void validate() throws ApiException, SensitivePropertyProtectionException {
        if (client == null) {
            final String msg = "The GCP KMS client failed to open, cannot validate key";
            throw new SensitivePropertyProtectionException(msg);
        }
        if (keyName == null) {
            final String msg = "The GCP KMS key provided is not provided/complete";
            throw new SensitivePropertyProtectionException(msg);
        }
        final CryptoKey key;
        final CryptoKeyVersion keyVersion;
        try {
            key = client.getCryptoKey(keyName);
            keyVersion = client.getCryptoKeyVersion(key.getPrimary().getName());
        } catch (final ApiException e) {
            throw new SensitivePropertyProtectionException("Encountered an error while fetching key details", e);
        }

        if (keyVersion.getState() != CryptoKeyVersion.CryptoKeyVersionState.ENABLED) {
            throw new SensitivePropertyProtectionException("The key is not enabled");
        }
    }

    /**
     * Checks if we have the required key properties for GCP Cloud KMS and loads it into {@link #keyName}.
     * Will load null if key is not present.
     * Note: This function does not verify if the key is correctly formatted/valid.
     * @param props the properties representing bootstrap-gcp.conf.
     */
    private void loadRequiredGCPProperties(final BootstrapProperties props) {
        if (props != null) {
            final String projectId = props.getProperty(PROJECT_ID_PROPS_NAME);
            final String locationId = props.getProperty(LOCATION_ID_PROPS_NAME);
            final String keyRingId = props.getProperty(KEYRING_ID_PROPS_NAME);
            final String keyId = props.getProperty(KEY_ID_PROPS_NAME);
            if (StringUtils.isNoneBlank(projectId, locationId, keyRingId, keyId)) {
                keyName = CryptoKeyName.of(projectId, locationId, keyRingId, keyId);
            }
        }
    }

    /**
     * Checks bootstrap.conf to check if BootstrapPropertyKey.GCP_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF property is
     * configured to the bootstrap-gcp.conf file. Also will load bootstrap-gcp.conf to {@link #gcpBootstrapProperties}.
     * @param bootstrapProperties BootstrapProperties object corresponding to bootstrap.conf.
     * @return BootstrapProperties object corresponding to bootstrap-gcp.conf, null otherwise.
     */
    private BootstrapProperties getGCPBootstrapProperties(final BootstrapProperties bootstrapProperties) {
        final BootstrapProperties cloudBootstrapProperties;

        // Load the bootstrap-gcp.conf file based on path specified in
        // "nifi.bootstrap.protection.gcp.kms.conf" property of bootstrap.conf
        final String filePath = bootstrapProperties.getProperty(BootstrapPropertyKey.GCP_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF).orElse(null);
        if (StringUtils.isBlank(filePath)) {
            logger.warn("GCP KMS properties file path not configured in bootstrap properties");
            return null;
        }

        try {
            cloudBootstrapProperties = AbstractBootstrapPropertiesLoader.loadBootstrapProperties(
                    Paths.get(filePath), GCP_PREFIX);
        } catch (final IOException e) {
            throw new SensitivePropertyProtectionException("Could not load " + filePath, e);
        }

        return cloudBootstrapProperties;
    }

    /**
     * Checks bootstrap-gcp.conf for the required configurations for Google Cloud KMS encrypt/decrypt operations.
     * @return true if bootstrap-gcp.conf contains the required properties for GCP KMS SPP, false otherwise.
     */
    private boolean hasRequiredGCPProperties() {
        if (gcpBootstrapProperties == null) {
            return false;
        }

        final String projectId = gcpBootstrapProperties.getProperty(PROJECT_ID_PROPS_NAME);
        final String locationId = gcpBootstrapProperties.getProperty(LOCATION_ID_PROPS_NAME);
        final String keyRingId = gcpBootstrapProperties.getProperty(KEYRING_ID_PROPS_NAME);
        final String keyId = gcpBootstrapProperties.getProperty(KEY_ID_PROPS_NAME);

        // Note: the following does not verify if the properties are valid properties, they only verify if
        // the properties are configured in bootstrap-gcp.conf.
        return StringUtils.isNoneBlank(projectId, locationId, keyRingId, keyId);
    }

    @Override
    public boolean isSupported() {
        return hasRequiredGCPProperties();
    }

    @Override
    protected PropertyProtectionScheme getProtectionScheme() {
        return PropertyProtectionScheme.GCP_KMS;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider.
     */
    @Override
    public String getName() {
        return PropertyProtectionScheme.GCP_KMS.getName();
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property.
     */
    @Override
    public String getIdentifierKey() {
        return PropertyProtectionScheme.GCP_KMS.getIdentifier();
    }

    /**
     * Returns the ciphertext blob of this value encrypted using a key stored in GCP KMS.
     * @return the ciphertext blob to persist in the {@code nifi.properties} file.
     */
    private byte[] encrypt(final byte[] input) throws IOException {
        final EncryptResponse response = client.encrypt(keyName, ByteString.copyFrom(input));
        return response.getCiphertext().toByteArray();
    }

    /**
     * Returns the value corresponding to a ciphertext blob decrypted using a key stored in GCP KMS.
     * @return the "unprotected" byte[] of this value, which could be used by the application.
     */
    private byte[] decrypt(final byte[] input) throws IOException {
        final DecryptResponse response = client.decrypt(keyName, ByteString.copyFrom(input));
        return response.getPlaintext().toByteArray();
    }

    /**
     * Checks if the client is open and if not, initializes the client and validates the key required for GCP KMS.
     */
    private void checkAndInitializeClient() throws SensitivePropertyProtectionException {
        if (client == null) {
            try {
                initializeClient();
                validate();
            } catch (final SensitivePropertyProtectionException e) {
                throw new SensitivePropertyProtectionException("Error initializing the GCP KMS client", e);
            }
        }
    }

    /**
     * Returns the "protected" form of this value. This is a form which can safely be persisted in the {@code nifi.properties} file without compromising the value.
     * Encrypts a sensitive value using a key managed by Google Cloud Key Management Service.
     *
     * @param unprotectedValue the sensitive value.
     * @param context The context of the value (ignored in this implementation)
     * @return the value to persist in the {@code nifi.properties} file.
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt a blank value");
        }

        checkAndInitializeClient();

        try {
            byte[] plainBytes = unprotectedValue.getBytes(PROPERTY_CHARSET);
            byte[] cipherBytes = encrypt(plainBytes);
            return Base64.getEncoder().encodeToString(cipherBytes);
        } catch (final IOException | ApiException e) {
            throw new SensitivePropertyProtectionException("Encrypt failed", e);
        }
    }

    /**
     * Returns the "unprotected" form of this value. This is the raw sensitive value which is used by the application logic.
     * Decrypts a secured value from a ciphertext using a key managed by Google Cloud Key Management Service.
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file.
     * @param context The context of the value (ignored in this implementation)
     * @return the raw value to be used by the application.
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(protectedValue)) {
            throw new IllegalArgumentException("Cannot decrypt a blank value");
        }

        checkAndInitializeClient();

        try {
            byte[] cipherBytes = Base64.getDecoder().decode(protectedValue);
            byte[] plainBytes = decrypt(cipherBytes);
            return new String(plainBytes, PROPERTY_CHARSET);
        } catch (final IOException | ApiException e) {
            throw new SensitivePropertyProtectionException("Decrypt failed", e);
        }
    }

    /**
     * Closes GCP KMS client that may have been opened.
     */
    @Override
    public void cleanUp() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
