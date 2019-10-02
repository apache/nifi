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
package org.apache.nifi.processors.aws.s3.encryption;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationResult;

import javax.crypto.spec.SecretKeySpec;

/**
 * This strategy uses a client master key to perform client-side encryption.   Use this strategy when you want the client to perform the encryption,
 * (thus incurring the cost of processing) and when you want to manage the key material yourself.
 *
 * See https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html#client-side-encryption-client-side-master-key-intro
 *
 */
public class ClientSideCEncryptionStrategy implements S3EncryptionStrategy {
    /**
     * Create an encryption client.
     *
     * @param credentialsProvider AWS credentials provider.
     * @param clientConfiguration Client configuration
     * @param kmsRegion not used by this encryption strategy
     * @param keyIdOrMaterial client master key, always base64 encoded
     * @return AWS S3 client
     */
    @Override
    public AmazonS3Client createEncryptionClient(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, String kmsRegion, String keyIdOrMaterial) {
        ValidationResult keyValidationResult = validateKey(keyIdOrMaterial);
        if (!keyValidationResult.isValid()) {
            throw new IllegalArgumentException("Invalid client key; " + keyValidationResult.getExplanation());
        }

        byte[] keyMaterial = Base64.decodeBase64(keyIdOrMaterial);
        SecretKeySpec symmetricKey = new SecretKeySpec(keyMaterial, "AES");
        StaticEncryptionMaterialsProvider encryptionMaterialsProvider = new StaticEncryptionMaterialsProvider(new EncryptionMaterials(symmetricKey));

        AmazonS3EncryptionClient client = new AmazonS3EncryptionClient(credentialsProvider, encryptionMaterialsProvider);

        return client;
    }

    @Override
    public ValidationResult validateKey(String keyValue) {
        if (StringUtils.isBlank(keyValue)) {
            return new ValidationResult.Builder()
                    .subject("Key Material")
                    .valid(false)
                    .explanation("it is empty")
                    .build();
        }

        byte[] keyMaterial;

        try {
            if (!Base64.isBase64(keyValue)) {
                throw new Exception();
            }
            keyMaterial = Base64.decodeBase64(keyValue);
        } catch (Exception e) {
            return new ValidationResult.Builder()
                    .subject("Key Material")
                    .valid(false)
                    .explanation("it is not in Base64 encoded form")
                    .build();
        }

        if (!(keyMaterial.length == 32 || keyMaterial.length == 24 || keyMaterial.length == 16)) {
            return new ValidationResult.Builder()
                    .subject("Key Material")
                    .valid(false)
                    .explanation("it is not a Base64 encoded AES-256, AES-192 or AES-128 key")
                    .build();
        }

        return new ValidationResult.Builder().valid(true).build();
    }
}
