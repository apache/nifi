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
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

/**
 * This strategy uses a customer key to perform server-side encryption.  Use this strategy when you want the server to perform the encryption,
 * (meaning you pay cost of processing) and when you want to manage the key material yourself.
 *
 * See <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html">Server Side Encryption Customer Keys</a>.
 */
public class ServerSideCEncryptionStrategy implements S3EncryptionStrategy {

    private static final String SSE_CUSTOMER_ALGORITHM = "AES256";

    @Override
    public void configurePutObjectRequest(PutObjectRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM);
        requestBuilder.sseCustomerKey(keySpec.material());
        requestBuilder.sseCustomerKeyMD5(keySpec.md5());
    }

    @Override
    public void configureCreateMultipartUploadRequest(CreateMultipartUploadRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM);
        requestBuilder.sseCustomerKey(keySpec.material());
        requestBuilder.sseCustomerKeyMD5(keySpec.md5());
    }

    @Override
    public void configureGetObjectRequest(GetObjectRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM);
        requestBuilder.sseCustomerKey(keySpec.material());
        requestBuilder.sseCustomerKeyMD5(keySpec.md5());
    }

    @Override
    public void configureUploadPartRequest(UploadPartRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
        requestBuilder.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM);
        requestBuilder.sseCustomerKey(keySpec.material());
        requestBuilder.sseCustomerKeyMD5(keySpec.md5());
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

        if (keyMaterial.length != 32) {
            return new ValidationResult.Builder()
                    .subject("Key Material")
                    .valid(false)
                    .explanation("it is not a Base64 encoded AES-256 key")
                    .build();
        }

        return new ValidationResult.Builder().valid(true).build();
    }
}
