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
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/**
 * This strategy uses a KMS key to perform server-side encryption.  Use this strategy when you want the server to perform the encryption,
 * (meaning you pay the cost of processing) and when you want to use a KMS key.
 *
 * See <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html">Using KMS Encryption</a>
 *
 */
public class ServerSideKMSEncryptionStrategy implements S3EncryptionStrategy {
    @Override
    public void configurePutObjectRequest(PutObjectRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
        requestBuilder.ssekmsKeyId(keySpec.kmsId());
    }

    @Override
    public void configureCreateMultipartUploadRequest(CreateMultipartUploadRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
        requestBuilder.ssekmsKeyId(keySpec.kmsId());
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
