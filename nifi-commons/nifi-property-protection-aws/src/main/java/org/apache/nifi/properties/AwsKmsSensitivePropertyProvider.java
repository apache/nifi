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

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;

import java.util.Properties;

/**
 * Amazon Web Services Key Management Service Sensitive Property Provider
 */
public class AwsKmsSensitivePropertyProvider extends ClientBasedEncodedSensitivePropertyProvider<KmsClient> {
    protected static final String KEY_ID_PROPERTY = "aws.kms.key.id";

    private static final String IDENTIFIER_KEY = "aws/kms";

    AwsKmsSensitivePropertyProvider(final KmsClient kmsClient, final Properties properties) throws SensitivePropertyProtectionException {
        super(kmsClient, properties);
    }

    @Override
    public String getIdentifierKey() {
        return IDENTIFIER_KEY;
    }

    /**
     * Close KMS Client when configured
     */
    @Override
    public void cleanUp() {
        final KmsClient kmsClient = getClient();
        if (kmsClient == null) {
            logger.debug("AWS KMS Client not configured");
        } else {
            kmsClient.close();
        }
    }

    /**
     * Validate Client and Key Identifier status when client is configured
     *
     * @param kmsClient KMS Client
     */
    @Override
    protected void validate(final KmsClient kmsClient) {
        if (kmsClient == null) {
            logger.debug("AWS KMS Client not configured");
        } else {
            final String keyId = getKeyId();
            try {
                final DescribeKeyRequest describeKeyRequest = DescribeKeyRequest.builder()
                        .keyId(keyId)
                        .build();
                final DescribeKeyResponse describeKeyResponse = kmsClient.describeKey(describeKeyRequest);
                final KeyMetadata keyMetadata = describeKeyResponse.keyMetadata();
                if (keyMetadata.enabled()) {
                    logger.info("AWS KMS Key [{}] Enabled", keyId);
                } else {
                    throw new SensitivePropertyProtectionException(String.format("AWS KMS Key [%s] Disabled", keyId));
                }
            } catch (final RuntimeException e) {
                throw new SensitivePropertyProtectionException(String.format("AWS KMS Key [%s] Validation Failed", keyId), e);
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
        final SdkBytes plainBytes = SdkBytes.fromByteArray(bytes);
        final EncryptRequest encryptRequest = EncryptRequest.builder()
                .keyId(getKeyId())
                .plaintext(plainBytes)
                .build();
        final EncryptResponse response = getClient().encrypt(encryptRequest);
        final SdkBytes encryptedData = response.ciphertextBlob();
        return encryptedData.asByteArray();
    }

    /**
     * Get decrypted bytes
     *
     * @param bytes Encrypted bytes
     * @return Decrypted bytes
     */
    @Override
    protected byte[] getDecrypted(final byte[] bytes) {
        final SdkBytes cipherBytes = SdkBytes.fromByteArray(bytes);
        final DecryptRequest decryptRequest = DecryptRequest.builder()
                .ciphertextBlob(cipherBytes)
                .keyId(getKeyId())
                .build();
        final DecryptResponse response = getClient().decrypt(decryptRequest);
        final SdkBytes decryptedData = response.plaintext();
        return decryptedData.asByteArray();
    }

    private String getKeyId() {
        return getProperties().getProperty(KEY_ID_PROPERTY);
    }
}
