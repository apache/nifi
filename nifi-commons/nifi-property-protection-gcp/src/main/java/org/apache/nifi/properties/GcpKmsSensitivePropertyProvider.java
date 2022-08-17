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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Google Cloud Platform Key Management Service Sensitive Property Provider
 */
public class GcpKmsSensitivePropertyProvider extends ClientBasedEncodedSensitivePropertyProvider<KeyManagementServiceClient> {
    protected static final String PROJECT_PROPERTY = "gcp.kms.project";
    protected static final String LOCATION_PROPERTY = "gcp.kms.location";
    protected static final String KEYRING_PROPERTY = "gcp.kms.keyring";
    protected static final String KEY_PROPERTY = "gcp.kms.key";

    private static final String SCHEME_BASE_PATH = "gcp/kms";

    private CryptoKeyName cryptoKeyName;

    GcpKmsSensitivePropertyProvider(final KeyManagementServiceClient keyManagementServiceClient, final Properties properties) {
        super(keyManagementServiceClient, properties);
    }

    @Override
    public String getIdentifierKey() {
        return SCHEME_BASE_PATH;
    }

    /**
     * Close Client when configured
     */
    @Override
    public void cleanUp() {
        final KeyManagementServiceClient keyManagementServiceClient = getClient();
        if (keyManagementServiceClient == null) {
            logger.debug("GCP KMS Client not configured");
        } else {
            keyManagementServiceClient.close();
        }
    }

    /**
     * Validate Client and Key Operations with Encryption Algorithm when configured
     *
     * @param keyManagementServiceClient Key Management Service Client
     */
    @Override
    protected void validate(final KeyManagementServiceClient keyManagementServiceClient) {
        if (keyManagementServiceClient == null) {
            logger.debug("GCP KMS Client not configured");
        } else {
            final String project = getProperties().getProperty(PROJECT_PROPERTY);
            final String location = getProperties().getProperty(LOCATION_PROPERTY);
            final String keyring = getProperties().getProperty(KEYRING_PROPERTY);
            final String key = getProperties().getProperty(KEY_PROPERTY);
            if (StringUtils.isNoneBlank(project, location, keyring, key)) {
                cryptoKeyName = CryptoKeyName.of(project, location, keyring, key);
                try {
                    final CryptoKey cryptoKey = keyManagementServiceClient.getCryptoKey(cryptoKeyName);
                    final CryptoKeyVersion cryptoKeyVersion = cryptoKey.getPrimary();
                    if (CryptoKeyVersion.CryptoKeyVersionState.ENABLED == cryptoKeyVersion.getState()) {
                        logger.info("GCP KMS Crypto Key [{}] Validated", cryptoKeyName);
                    } else {
                        throw new SensitivePropertyProtectionException(String.format("GCP KMS Crypto Key [%s] Disabled", cryptoKeyName));
                    }
                } catch (final ApiException e) {
                    throw new SensitivePropertyProtectionException(String.format("GCP KMS Crypto Key [%s] Validation Failed", cryptoKeyName), e);
                }
            } else {
                throw new SensitivePropertyProtectionException("GCP KMS Missing Required Properties");
            }
        }
    }

    /**
     * Get encrypted bytes
     *
     * @param bytes Unprotected bytes
     * @return Encrypted bytes
     */
    @Override
    protected byte[] getEncrypted(final byte[] bytes) {
        final EncryptResponse encryptResponse = getClient().encrypt(cryptoKeyName, ByteString.copyFrom(bytes));
        return encryptResponse.getCiphertext().toByteArray();
    }

    /**
     * Get decrypted bytes
     *
     * @param bytes Encrypted bytes
     * @return Decrypted bytes
     */
    @Override
    protected byte[] getDecrypted(final byte[] bytes) {
        final DecryptResponse decryptResponse = getClient().decrypt(cryptoKeyName, ByteString.copyFrom(bytes));
        return decryptResponse.getPlaintext().toByteArray();
    }
}
