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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.nifi.components.ValidationResult;

import java.util.function.Consumer;

/**
 * This interface defines the API for S3 encryption strategies.  The methods have empty defaults
 * to minimize the burden on implementations.
 *
 */
public interface S3EncryptionStrategy {

    /**
     * Configure a {@link PutObjectRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     * @param keyValue the key id or key material.
     */
    default void configurePutObjectRequest(PutObjectRequest request, ObjectMetadata objectMetadata, String keyValue) {
    }

    /**
     * Configure an {@link InitiateMultipartUploadRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     * @param keyValue the key id or key material.
     */
    default void configureInitiateMultipartUploadRequest(InitiateMultipartUploadRequest request, ObjectMetadata objectMetadata, String keyValue) {
    }

    /**
     * Configure a {@link GetObjectRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     * @param keyValue the key id or key material.
     */
    default void configureGetObjectRequest(GetObjectRequest request, ObjectMetadata objectMetadata, String keyValue) {
    }

    /**
     * Configure an {@link UploadPartRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     * @param keyValue the key id or key material.
     */
    default void configureUploadPartRequest(UploadPartRequest request, ObjectMetadata objectMetadata, String keyValue) {
    }

    /**
     * Create an S3 encryption client.
     *
     */
    default AmazonS3 createEncryptionClient(final Consumer<AmazonS3Builder<?, ?>> clientBuilder, String kmsRegion, String keyIdOrMaterial) {
        return null;
    }

    /**
     * Validate a key id or key material.
     *
     * @param keyValue key id or key material to validate.
     * @return ValidationResult instance.
     */
    default ValidationResult validateKey(String keyValue) {
        return new ValidationResult.Builder().valid(true).build();
    }
}
