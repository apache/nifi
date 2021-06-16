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

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;


public class TestS3EncryptionStrategies {

    private String randomKeyMaterial = "";
    private String randomKeyId = "mock-key-id";
    private String kmsRegion = "us-west-1";

    private ObjectMetadata metadata = null;
    private PutObjectRequest putObjectRequest = null;
    private InitiateMultipartUploadRequest initUploadRequest = null;
    private GetObjectRequest getObjectRequest = null;
    private UploadPartRequest uploadPartRequest = null;

    @Before
    public void setup() {
        byte[] keyRawBytes = new byte[32];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(keyRawBytes);
        randomKeyMaterial = Base64.encodeBase64String(keyRawBytes);

        metadata = new ObjectMetadata();
        putObjectRequest = new PutObjectRequest("", "", "");
        initUploadRequest = new InitiateMultipartUploadRequest("", "");
        getObjectRequest = new GetObjectRequest("", "");
        uploadPartRequest = new UploadPartRequest();
    }

    @Test
    public void testClientSideKMSEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ClientSideKMSEncryptionStrategy();

        // This shows that the strategy builds a client:
        Assert.assertNotNull(strategy.createEncryptionClient(null, null, kmsRegion, randomKeyMaterial));

        // This shows that the strategy does not modify the metadata or any of the requests:
        Assert.assertNull(metadata.getSSEAlgorithm());
        Assert.assertNull(putObjectRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(putObjectRequest.getSSECustomerKey());

        Assert.assertNull(initUploadRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(initUploadRequest.getSSECustomerKey());

        Assert.assertNull(getObjectRequest.getSSECustomerKey());

        Assert.assertNull(uploadPartRequest.getSSECustomerKey());
    }

    @Test
    public void testClientSideCEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ClientSideCEncryptionStrategy();

        // This shows that the strategy builds a client:
        Assert.assertNotNull(strategy.createEncryptionClient(null, null, null, randomKeyMaterial));

        // This shows that the strategy does not modify the metadata or any of the requests:
        Assert.assertNull(metadata.getSSEAlgorithm());
        Assert.assertNull(putObjectRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(putObjectRequest.getSSECustomerKey());

        Assert.assertNull(initUploadRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(initUploadRequest.getSSECustomerKey());

        Assert.assertNull(getObjectRequest.getSSECustomerKey());

        Assert.assertNull(uploadPartRequest.getSSECustomerKey());
    }

    @Test
    public void testServerSideCEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ServerSideCEncryptionStrategy();

        // This shows that the strategy does *not* build a client:
        Assert.assertNull(strategy.createEncryptionClient(null, null, null, ""));

        // This shows that the strategy sets the SSE customer key as expected:
        strategy.configurePutObjectRequest(putObjectRequest, metadata, randomKeyMaterial);
        Assert.assertEquals(randomKeyMaterial, putObjectRequest.getSSECustomerKey().getKey());
        Assert.assertNull(putObjectRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(metadata.getSSEAlgorithm());

        // Same for InitiateMultipartUploadRequest:
        strategy.configureInitiateMultipartUploadRequest(initUploadRequest, metadata, randomKeyMaterial);
        Assert.assertEquals(randomKeyMaterial, initUploadRequest.getSSECustomerKey().getKey());
        Assert.assertNull(initUploadRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(metadata.getSSEAlgorithm());

        // Same for GetObjectRequest:
        strategy.configureGetObjectRequest(getObjectRequest, metadata, randomKeyMaterial);
        Assert.assertEquals(randomKeyMaterial, initUploadRequest.getSSECustomerKey().getKey());
        Assert.assertNull(metadata.getSSEAlgorithm());

        // Same for UploadPartRequest:
        strategy.configureUploadPartRequest(uploadPartRequest, metadata, randomKeyMaterial);
        Assert.assertEquals(randomKeyMaterial, uploadPartRequest.getSSECustomerKey().getKey());
        Assert.assertNull(metadata.getSSEAlgorithm());
    }

    @Test
    public void testServerSideKMSEncryptionStrategy() {
        S3EncryptionStrategy strategy = new ServerSideKMSEncryptionStrategy();

        // This shows that the strategy does *not* build a client:
        Assert.assertNull(strategy.createEncryptionClient(null, null, null, null));

        // This shows that the strategy sets the SSE KMS key id as expected:
        strategy.configurePutObjectRequest(putObjectRequest, metadata, randomKeyId);
        Assert.assertEquals(randomKeyId, putObjectRequest.getSSEAwsKeyManagementParams().getAwsKmsKeyId());
        Assert.assertNull(putObjectRequest.getSSECustomerKey());
        Assert.assertNull(metadata.getSSEAlgorithm());

        // Same for InitiateMultipartUploadRequest:
        strategy.configureInitiateMultipartUploadRequest(initUploadRequest, metadata, randomKeyId);
        Assert.assertEquals(randomKeyId, initUploadRequest.getSSEAwsKeyManagementParams().getAwsKmsKeyId());
        Assert.assertNull(initUploadRequest.getSSECustomerKey());
        Assert.assertNull(metadata.getSSEAlgorithm());
    }

    @Test
    public void testServerSideS3EncryptionStrategy() {
        S3EncryptionStrategy strategy = new ServerSideS3EncryptionStrategy();

        // This shows that the strategy does *not* build a client:
        Assert.assertNull(strategy.createEncryptionClient(null, null, null, null));

        // This shows that the strategy sets the SSE algorithm field as expected:
        strategy.configurePutObjectRequest(putObjectRequest, metadata, null);
        Assert.assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, metadata.getSSEAlgorithm());

        // Same for InitiateMultipartUploadRequest:
        strategy.configureInitiateMultipartUploadRequest(initUploadRequest, metadata, null);
        Assert.assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, metadata.getSSEAlgorithm());
    }

    @Test
    public void testNoOpEncryptionStrategy() {
        S3EncryptionStrategy strategy = new NoOpEncryptionStrategy();

        // This shows that the strategy does *not* build a client:
        Assert.assertNull(strategy.createEncryptionClient(null, null, "", ""));

        // This shows the request and metadata start with various null objects:
        Assert.assertNull(metadata.getSSEAlgorithm());
        Assert.assertNull(putObjectRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(putObjectRequest.getSSECustomerKey());

        // Act:
        strategy.configurePutObjectRequest(putObjectRequest, metadata, "");

        // This shows that the request and metadata were not changed:
        Assert.assertNull(metadata.getSSEAlgorithm());
        Assert.assertNull(putObjectRequest.getSSEAwsKeyManagementParams());
        Assert.assertNull(putObjectRequest.getSSECustomerKey());
    }
}
