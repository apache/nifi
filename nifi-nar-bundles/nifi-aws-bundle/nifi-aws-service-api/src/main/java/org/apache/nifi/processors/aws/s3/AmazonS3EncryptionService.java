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
package org.apache.nifi.processors.aws.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.nifi.controller.ControllerService;

/**
 * This interface defines how clients interact with an S3 encryption service.
 */
public interface AmazonS3EncryptionService extends ControllerService {

    String STRATEGY_NAME_NONE = "NONE";
    String STRATEGY_NAME_SSE_S3 = "SSE_S3";
    String STRATEGY_NAME_SSE_KMS = "SSE_KMS";
    String STRATEGY_NAME_SSE_C = "SSE_C";
    String STRATEGY_NAME_CSE_KMS = "CSE_KMS";
    String STRATEGY_NAME_CSE_C = "CSE_C";

    /**
     * Configure a {@link PutObjectRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configurePutObjectRequest(PutObjectRequest request, ObjectMetadata objectMetadata);

    /**
     * Configure an {@link InitiateMultipartUploadRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configureInitiateMultipartUploadRequest(InitiateMultipartUploadRequest request, ObjectMetadata objectMetadata);

    /**
     * Configure a {@link GetObjectRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configureGetObjectRequest(GetObjectRequest request, ObjectMetadata objectMetadata);

    /**
     * Configure an {@link UploadPartRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configureUploadPartRequest(UploadPartRequest request, ObjectMetadata objectMetadata);

    /**
     * Create an S3 encryption client.
     *
     * @param credentialsProvider AWS credentials provider.
     * @param clientConfiguration Client configuration.
     * @return {@link AmazonS3Client}, perhaps an {@link com.amazonaws.services.s3.AmazonS3EncryptionClient}
     */
    AmazonS3Client createEncryptionClient(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration);

    /**
     * @return The KMS region associated with the service, as a String.
     */
    String getKmsRegion();

    /**
     * @return The name of the encryption strategy associated with the service.
     */
    String getStrategyName();

    /**
     * @return The display name of the encryption strategy associated with the service.
     */
    String getStrategyDisplayName();
}
