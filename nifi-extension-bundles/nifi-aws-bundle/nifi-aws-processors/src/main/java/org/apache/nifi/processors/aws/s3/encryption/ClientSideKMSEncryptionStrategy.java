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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationResult;
import software.amazon.encryption.s3.S3EncryptionClient;

/**
 * This strategy uses KMS key id to perform client-side encryption.  Use this strategy when you want the client to perform the encryption,
 * (thus incurring the cost of processing) and manage the key in a KMS instance.
 *
 * See https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html#client-side-encryption-kms-managed-master-key-intro
 *
 */
public class ClientSideKMSEncryptionStrategy implements S3EncryptionStrategy {
    @Override
    public S3EncryptionClient.Builder createEncryptionClientBuilder(S3EncryptionKeySpec keySpec) {
        return S3EncryptionClient.builderV4()
                .kmsKeyId(keySpec.kmsId())
                .commitmentPolicy(keySpec.commitmentPolicy());
    }

    @Override
    public ValidationResult validateKeySpec(S3EncryptionKeySpec keySpec) {
        if (StringUtils.isBlank(keySpec.kmsId())) {
            return new ValidationResult.Builder()
                    .subject("KMS Key ID")
                    .valid(false)
                    .explanation("it is empty")
                    .build();
        }

        return new ValidationResult.Builder().valid(true).build();
    }
}
