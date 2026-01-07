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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.aws.s3.encryption.S3EncryptionTestUtil.createCustomerKeySpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestStandardS3EncryptionService {
    private StandardS3EncryptionService service;
    private String strategyName;
    private String keyMaterial;
    private String keyMaterialMd5;
    private String keyAlgorithm;

    @BeforeEach
    public void setup() throws InitializationException {
        service = new StandardS3EncryptionService();
        ConfigurationContext context = Mockito.mock(ConfigurationContext.class);

        S3EncryptionKeySpec keySpec = createCustomerKeySpec(256);

        strategyName = AmazonS3EncryptionService.STRATEGY_NAME_SSE_C;
        keyMaterial = keySpec.material();
        keyMaterialMd5 = keySpec.md5();
        keyAlgorithm = "AES256";

        Mockito.when(context.getProperty(StandardS3EncryptionService.ENCRYPTION_STRATEGY)).thenReturn(new MockPropertyValue(strategyName));
        Mockito.when(context.getProperty(StandardS3EncryptionService.KEY_MATERIAL)).thenReturn(new MockPropertyValue(keyMaterial));
        service.onConfigured(context);
    }

    @Test
    public void testServiceProperties() {
        assertEquals(service.getStrategyName(), strategyName);
    }

    @Test
    public void testRequests() {
        final GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder();
        final CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder();
        final PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder();
        final UploadPartRequest.Builder uploadPartRequestBuilder = UploadPartRequest.builder();

        service.configureGetObjectRequest(getObjectRequestBuilder);
        GetObjectRequest getObjectRequest = getObjectRequestBuilder.build();
        assertEquals(keyAlgorithm, getObjectRequest.sseCustomerAlgorithm());
        assertEquals(keyMaterial, getObjectRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, getObjectRequest.sseCustomerKeyMD5());

        service.configureUploadPartRequest(uploadPartRequestBuilder);
        UploadPartRequest uploadPartRequest = uploadPartRequestBuilder.build();
        assertEquals(keyAlgorithm, uploadPartRequest.sseCustomerAlgorithm());
        assertEquals(keyMaterial, uploadPartRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, uploadPartRequest.sseCustomerKeyMD5());

        service.configurePutObjectRequest(putObjectRequestBuilder);
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        assertEquals(keyAlgorithm, putObjectRequest.sseCustomerAlgorithm());
        assertEquals(keyMaterial, putObjectRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, putObjectRequest.sseCustomerKeyMD5());
        assertNull(putObjectRequest.serverSideEncryption());
        assertNull(putObjectRequest.ssekmsKeyId());

        service.configureCreateMultipartUploadRequest(createMultipartUploadRequestBuilder);
        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        assertEquals(keyAlgorithm, createMultipartUploadRequest.sseCustomerAlgorithm());
        assertEquals(keyMaterial, createMultipartUploadRequest.sseCustomerKey());
        assertEquals(keyMaterialMd5, createMultipartUploadRequest.sseCustomerKeyMD5());
        assertNull(createMultipartUploadRequest.serverSideEncryption());
        assertNull(createMultipartUploadRequest.ssekmsKeyId());
    }

    @Test
    public void testProperties() {
        List<PropertyDescriptor> properties = service.getSupportedPropertyDescriptors();
        assertEquals(4, properties.size());

        assertEquals(properties.get(0).getName(), StandardS3EncryptionService.ENCRYPTION_STRATEGY.getName());
        assertEquals(properties.get(1).getName(), StandardS3EncryptionService.KMS_KEY_ID.getName());
        assertEquals(properties.get(2).getName(), StandardS3EncryptionService.KEY_MATERIAL.getName());
        assertEquals(properties.get(3).getName(), StandardS3EncryptionService.COMMITMENT_POLICY.getName());
    }

    @Test
    void testMigration() {
        final Map<String, String> propertyValues = Map.of(
                StandardS3EncryptionService.ENCRYPTION_STRATEGY.getName(), strategyName,
                StandardS3EncryptionService.KEY_MATERIAL.getName(), keyMaterial
        );

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        final StandardS3EncryptionService standardS3EncryptionService = new StandardS3EncryptionService();
        standardS3EncryptionService.migrateProperties(configuration);

        Map<String, String> expected = Map.of(
                "encryption-strategy", StandardS3EncryptionService.ENCRYPTION_STRATEGY.getName()
        );

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expected, propertiesRenamed);
    }
}
