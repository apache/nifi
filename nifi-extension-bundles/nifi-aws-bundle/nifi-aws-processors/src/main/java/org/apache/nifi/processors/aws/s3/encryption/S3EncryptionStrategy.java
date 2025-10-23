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

import org.apache.nifi.components.ValidationResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.encryption.s3.S3EncryptionClient;

/**
 * This interface defines the API for S3 encryption strategies.  The methods have empty defaults
 * to minimize the burden on implementations.
 *
 */
public interface S3EncryptionStrategy {

    /**
     * Configure a {@link PutObjectRequest} for encryption.
     * @param requestBuilder the builder of the request to configure
     * @param keySpec key specification
     */
    default void configurePutObjectRequest(PutObjectRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
    }

    /**
     * Configure an {@link CreateMultipartUploadRequest} for encryption.
     * @param requestBuilder the builder of the request to configure
     * @param keySpec key specification
     */
    default void configureCreateMultipartUploadRequest(CreateMultipartUploadRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
    }

    /**
     * Configure a {@link GetObjectRequest} for encryption.
     * @param requestBuilder the builder of the request to configure
     * @param keySpec key specification
     */
    default void configureGetObjectRequest(GetObjectRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
    }

    /**
     * Configure an {@link UploadPartRequest} for encryption.
     * @param requestBuilder the builder of the request to configure
     * @param keySpec key specification
     */
    default void configureUploadPartRequest(UploadPartRequest.Builder requestBuilder, S3EncryptionKeySpec keySpec) {
    }

    /**
     * Create an S3 encryption client builder.
     *
     */
    default S3EncryptionClient.Builder createEncryptionClientBuilder(S3EncryptionKeySpec keySpec) {
        return null;
    }

    /**
     * Validate the key specification.
     *
     * @param keySpec key specification
     * @return ValidationResult instance
     */
    default ValidationResult validateKeySpec(S3EncryptionKeySpec keySpec) {
        return new ValidationResult.Builder().valid(true).build();
    }
}
