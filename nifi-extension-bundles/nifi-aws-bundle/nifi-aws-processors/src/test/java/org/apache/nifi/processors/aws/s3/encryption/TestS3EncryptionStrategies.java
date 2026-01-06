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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.encryption.s3.CommitmentPolicy;
import software.amazon.encryption.s3.S3EncryptionClient;

import static org.apache.nifi.processors.aws.s3.encryption.S3EncryptionTestUtil.createCustomerKeySpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestS3EncryptionStrategies {

    private PutObjectRequest.Builder putObjectRequestBuilder;
    private CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder;
    private GetObjectRequest.Builder getObjectRequestBuilder;
    private UploadPartRequest.Builder uploadPartRequestBuilder;

    @BeforeEach
    public void setup() {
        putObjectRequestBuilder = PutObjectRequest.builder();
        createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder();
        getObjectRequestBuilder = GetObjectRequest.builder();
        uploadPartRequestBuilder = UploadPartRequest.builder();
    }

    @Test
    public void testClientSideCEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ClientSideCEncryptionStrategy();

        S3EncryptionKeySpec keySpec = createCustomerKeySpec(256, CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT);

        // This shows that the strategy creates a client builder:
        S3EncryptionClient.Builder builder = strategy.createEncryptionClientBuilder(keySpec);
        assertNotNull(builder);

        // This shows that the strategy does not modify other requests:
        testPutObjectRequestNotModified(strategy, keySpec);
        testCreateMultipartUploadRequestBuilderNotModified(strategy, keySpec);
        testGetObjectRequestNotModified(strategy, keySpec);
        testUploadPartRequestBuilderNotModified(strategy, keySpec);
    }

    @Test
    public void testClientSideKMSEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ClientSideKMSEncryptionStrategy();

        String keyId = "key-id";
        S3EncryptionKeySpec keySpec = new S3EncryptionKeySpec(keyId, null, null, CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT);

        // This shows that the strategy creates a client builder:
        S3EncryptionClient.Builder builder = strategy.createEncryptionClientBuilder(keySpec);
        assertNotNull(builder);

        // This shows that the strategy does not modify other requests:
        testPutObjectRequestNotModified(strategy, keySpec);
        testCreateMultipartUploadRequestBuilderNotModified(strategy, keySpec);
        testGetObjectRequestNotModified(strategy, keySpec);
        testUploadPartRequestBuilderNotModified(strategy, keySpec);
    }

    @Test
    public void testServerSideCEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ServerSideCEncryptionStrategy();

        S3EncryptionKeySpec keySpec = createCustomerKeySpec(256);
        String keyMaterial = keySpec.material();
        String keyMaterialMd5 = keySpec.md5();
        String keyAlgorithm = "AES256";

        // This shows that the strategy sets the SSE customer key as expected:
        strategy.configurePutObjectRequest(putObjectRequestBuilder, keySpec);
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        assertEquals(keyMaterial, putObjectRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, putObjectRequest.sseCustomerKeyMD5());
        assertEquals(keyAlgorithm, putObjectRequest.sseCustomerAlgorithm());
        assertNull(putObjectRequest.serverSideEncryption());
        assertNull(putObjectRequest.ssekmsKeyId());

        // Same for CreateMultipartUploadRequest:
        strategy.configureCreateMultipartUploadRequest(createMultipartUploadRequestBuilder, keySpec);
        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        assertEquals(keyMaterial, createMultipartUploadRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, createMultipartUploadRequest.sseCustomerKeyMD5());
        assertEquals(keyAlgorithm, createMultipartUploadRequest.sseCustomerAlgorithm());
        assertNull(createMultipartUploadRequest.serverSideEncryption());
        assertNull(createMultipartUploadRequest.ssekmsKeyId());

        // Same for GetObjectRequest:
        strategy.configureGetObjectRequest(getObjectRequestBuilder, keySpec);
        GetObjectRequest getObjectRequest = getObjectRequestBuilder.build();
        assertEquals(keyMaterial, getObjectRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, getObjectRequest.sseCustomerKeyMD5());
        assertEquals(keyAlgorithm, getObjectRequest.sseCustomerAlgorithm());

        // Same for UploadPartRequest:
        strategy.configureUploadPartRequest(uploadPartRequestBuilder, keySpec);
        UploadPartRequest uploadPartRequest = uploadPartRequestBuilder.build();
        assertEquals(keyMaterial, uploadPartRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, uploadPartRequest.sseCustomerKeyMD5());
        assertEquals(keyAlgorithm, uploadPartRequest.sseCustomerAlgorithm());
        testEncryptionClientBuilderNotCreated(strategy, keySpec);
    }

    @Test
    public void testServerSideKMSEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ServerSideKMSEncryptionStrategy();

        String keyId = "key-id";
        S3EncryptionKeySpec keySpec = new S3EncryptionKeySpec(keyId, null, null, null);

        // This shows that the strategy sets the SSE KMS key id as expected:
        strategy.configurePutObjectRequest(putObjectRequestBuilder, keySpec);
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        assertEquals(ServerSideEncryption.AWS_KMS, putObjectRequest.serverSideEncryption());
        assertEquals(keyId, putObjectRequest.ssekmsKeyId());
        assertNull(putObjectRequest.sseCustomerKey());
        assertNull(putObjectRequest.sseCustomerKeyMD5());
        assertNull(putObjectRequest.sseCustomerAlgorithm());

        // Same for CreateMultipartUploadRequest:
        strategy.configureCreateMultipartUploadRequest(createMultipartUploadRequestBuilder, keySpec);
        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        assertEquals(ServerSideEncryption.AWS_KMS, createMultipartUploadRequest.serverSideEncryption());
        assertEquals(keyId, createMultipartUploadRequest.ssekmsKeyId());
        assertNull(createMultipartUploadRequest.sseCustomerKey());
        assertNull(createMultipartUploadRequest.sseCustomerKeyMD5());
        assertNull(createMultipartUploadRequest.sseCustomerAlgorithm());

        // This shows that the strategy does not modify other requests:
        testGetObjectRequestNotModified(strategy, keySpec);
        testUploadPartRequestBuilderNotModified(strategy, keySpec);
        testEncryptionClientBuilderNotCreated(strategy, keySpec);
    }

    @Test
    public void testServerSideS3EncryptionStrategy() {
        S3EncryptionStrategy strategy = new ServerSideS3EncryptionStrategy();

        S3EncryptionKeySpec keySpec = new S3EncryptionKeySpec(null, null, null, null);

        // This shows that the strategy sets the SSE algorithm field as expected:
        strategy.configurePutObjectRequest(putObjectRequestBuilder, keySpec);
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        assertEquals(ServerSideEncryption.AES256, putObjectRequest.serverSideEncryption());

        // Same for CreateMultipartUploadRequest:
        strategy.configureCreateMultipartUploadRequest(createMultipartUploadRequestBuilder, keySpec);
        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        assertEquals(ServerSideEncryption.AES256, createMultipartUploadRequest.serverSideEncryption());

        // This shows that the strategy does not modify other requests:
        testGetObjectRequestNotModified(strategy, keySpec);
        testUploadPartRequestBuilderNotModified(strategy, keySpec);
        testEncryptionClientBuilderNotCreated(strategy, keySpec);
    }

    @Test
    public void testNoOpEncryptionStrategy() {
        S3EncryptionStrategy strategy = new NoOpEncryptionStrategy();

        // This shows that the strategy does not modify other requests:
        testPutObjectRequestNotModified(strategy, null);
        testCreateMultipartUploadRequestBuilderNotModified(strategy, null);
        testGetObjectRequestNotModified(strategy, null);
        testUploadPartRequestBuilderNotModified(strategy, null);
        testEncryptionClientBuilderNotCreated(strategy, null);
    }

    private void testPutObjectRequestNotModified(S3EncryptionStrategy strategy, S3EncryptionKeySpec keySpec) {
        // Act:
        strategy.configurePutObjectRequest(putObjectRequestBuilder, keySpec);
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

        // This shows that the request was not changed:
        assertNull(putObjectRequest.ssekmsKeyId());
        assertNull(putObjectRequest.sseCustomerKey());
        assertNull(putObjectRequest.sseCustomerKeyMD5());
        assertNull(putObjectRequest.sseCustomerAlgorithm());
        assertNull(putObjectRequest.serverSideEncryption());
    }

    private void testCreateMultipartUploadRequestBuilderNotModified(S3EncryptionStrategy strategy, S3EncryptionKeySpec keySpec) {
        // Act:
        strategy.configureCreateMultipartUploadRequest(createMultipartUploadRequestBuilder, keySpec);
        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();

        // This shows that the request was not changed:
        assertNull(createMultipartUploadRequest.ssekmsKeyId());
        assertNull(createMultipartUploadRequest.sseCustomerKey());
        assertNull(createMultipartUploadRequest.sseCustomerKeyMD5());
        assertNull(createMultipartUploadRequest.sseCustomerAlgorithm());
        assertNull(createMultipartUploadRequest.serverSideEncryption());
    }

    private void testGetObjectRequestNotModified(S3EncryptionStrategy strategy, S3EncryptionKeySpec keySpec) {
        // Act:
        strategy.configureGetObjectRequest(getObjectRequestBuilder, keySpec);
        GetObjectRequest getObjectRequest = getObjectRequestBuilder.build();

        // This shows that the request was not changed:
        assertNull(getObjectRequest.sseCustomerKey());
        assertNull(getObjectRequest.sseCustomerKeyMD5());
        assertNull(getObjectRequest.sseCustomerAlgorithm());
    }

    private void testUploadPartRequestBuilderNotModified(S3EncryptionStrategy strategy, S3EncryptionKeySpec keySpec) {
        // Act:
        strategy.configureUploadPartRequest(uploadPartRequestBuilder, keySpec);
        UploadPartRequest uploadPartRequest = uploadPartRequestBuilder.build();

        // This shows that the request was not changed:
        assertNull(uploadPartRequest.sseCustomerKey());
        assertNull(uploadPartRequest.sseCustomerKeyMD5());
        assertNull(uploadPartRequest.sseCustomerAlgorithm());
    }

    private void testEncryptionClientBuilderNotCreated(S3EncryptionStrategy strategy, S3EncryptionKeySpec keySpec) {
        assertNull(strategy.createEncryptionClientBuilder(keySpec));
    }
}
