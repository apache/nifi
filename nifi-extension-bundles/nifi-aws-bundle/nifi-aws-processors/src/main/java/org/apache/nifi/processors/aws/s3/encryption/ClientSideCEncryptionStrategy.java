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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationResult;
import software.amazon.encryption.s3.S3EncryptionClient;

import javax.crypto.spec.SecretKeySpec;

/**
 * This strategy uses a client master key to perform client-side encryption.   Use this strategy when you want the client to perform the encryption,
 * (thus incurring the cost of processing) and when you want to manage the key material yourself.
 *
 * See https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html#client-side-encryption-client-side-master-key-intro
 *
 */
public class ClientSideCEncryptionStrategy implements S3EncryptionStrategy {
    @Override
    public S3EncryptionClient.Builder createEncryptionClientBuilder(S3EncryptionKeySpec keySpec) {
        final ValidationResult keyValidationResult = validateKeySpec(keySpec);
        if (!keyValidationResult.isValid()) {
            throw new IllegalArgumentException("Invalid client key; " + keyValidationResult.getExplanation());
        }

        final byte[] keyMaterial = Base64.decodeBase64(keySpec.material());
        final SecretKeySpec symmetricKey = new SecretKeySpec(keyMaterial, "AES");

        return S3EncryptionClient.builderV4()
                .aesKey(symmetricKey)
                .commitmentPolicy(keySpec.commitmentPolicy());
    }

    @Override
    public ValidationResult validateKeySpec(S3EncryptionKeySpec keySpec) {
        if (StringUtils.isBlank(keySpec.material())) {
            return new ValidationResult.Builder()
                    .subject("Key Material")
                    .valid(false)
                    .explanation("it is empty")
                    .build();
        }

        byte[] keyMaterial;

        try {
            if (!Base64.isBase64(keySpec.material())) {
                throw new Exception();
            }
            keyMaterial = Base64.decodeBase64(keySpec.material());
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
