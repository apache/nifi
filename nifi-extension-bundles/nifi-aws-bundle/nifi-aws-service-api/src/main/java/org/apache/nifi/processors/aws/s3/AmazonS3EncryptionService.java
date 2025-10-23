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

import org.apache.nifi.controller.ControllerService;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.encryption.s3.S3EncryptionClient;

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
     * @param requestBuilder the builder of the request to configure.
     */
    void configurePutObjectRequest(PutObjectRequest.Builder requestBuilder);

    /**
     * Configure an {@link CreateMultipartUploadRequest} for encryption.
     * @param requestBuilder the builder of the request to configure.
     */
    void configureCreateMultipartUploadRequest(CreateMultipartUploadRequest.Builder requestBuilder);

    /**
     * Configure a {@link GetObjectRequest} for encryption.
     * @param requestBuilder the builder of the request to configure.
     */
    void configureGetObjectRequest(GetObjectRequest.Builder requestBuilder);

    /**
     * Configure an {@link UploadPartRequest} for encryption.
     * @param requestBuilder the builder of the request to configure.
     */
    void configureUploadPartRequest(UploadPartRequest.Builder requestBuilder);

    /**
     * Create an S3 encryption client builder.
     */
    S3EncryptionClient.Builder createEncryptionClientBuilder();

    /**
     * @return The name of the encryption strategy associated with the service.
     */
    String getStrategyName();

    /**
     * @return The display name of the encryption strategy associated with the service.
     */
    String getStrategyDisplayName();
}
