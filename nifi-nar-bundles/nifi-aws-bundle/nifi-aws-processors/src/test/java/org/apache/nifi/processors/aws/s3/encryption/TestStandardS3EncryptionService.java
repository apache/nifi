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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.List;


public class TestStandardS3EncryptionService {
    private StandardS3EncryptionService service;
    private ConfigurationContext context;
    private String strategyName;
    private String keyIdOrMaterial;
    private String kmsRegion;

    @Before
    public void setup() throws InitializationException {
        service = new StandardS3EncryptionService();
        context = Mockito.mock(ConfigurationContext.class);

        strategyName = AmazonS3EncryptionService.STRATEGY_NAME_NONE;
        keyIdOrMaterial = "test-key-id";
        kmsRegion = "us-west-1";

        Mockito.when(context.getProperty(StandardS3EncryptionService.ENCRYPTION_STRATEGY)).thenReturn(new MockPropertyValue(strategyName));
        Mockito.when(context.getProperty(StandardS3EncryptionService.ENCRYPTION_VALUE)).thenReturn(new MockPropertyValue(keyIdOrMaterial));
        Mockito.when(context.getProperty(StandardS3EncryptionService.KMS_REGION)).thenReturn(new MockPropertyValue(kmsRegion));
        service.onConfigured(context);
    }

    @Test
    public void testServiceProperties() {
        Assert.assertEquals(service.getKmsRegion(), kmsRegion);
        Assert.assertEquals(service.getStrategyName(), strategyName);
    }

    @Test
    public void testCreateClientReturnsNull() {
        Assert.assertNull(service.createEncryptionClient(null, null));
    }

    @Test
    public void testRequests() {
        final ObjectMetadata metadata = new ObjectMetadata();
        final GetObjectRequest getObjectRequest = new GetObjectRequest("", "");
        final InitiateMultipartUploadRequest initUploadRequest = new InitiateMultipartUploadRequest("", "");
        final PutObjectRequest putObjectRequest = new PutObjectRequest("", "", "");
        final UploadPartRequest uploadPartRequest = new UploadPartRequest();

        service.configureGetObjectRequest(getObjectRequest, metadata);
        Assert.assertNull(getObjectRequest.getSSECustomerKey());
        Assert.assertNull(metadata.getSSEAlgorithm());

        service.configureUploadPartRequest(uploadPartRequest, metadata);
        Assert.assertNull(uploadPartRequest.getSSECustomerKey());
        Assert.assertNull(metadata.getSSEAlgorithm());

        service.configurePutObjectRequest(putObjectRequest, metadata);
        Assert.assertNull(putObjectRequest.getSSECustomerKey());
        Assert.assertNull(metadata.getSSEAlgorithm());

        service.configureInitiateMultipartUploadRequest(initUploadRequest, metadata);
        Assert.assertNull(initUploadRequest.getSSECustomerKey());
        Assert.assertNull(metadata.getSSEAlgorithm());
    }

    @Test
    public void testProperties() {
        List<PropertyDescriptor> properties = service.getSupportedPropertyDescriptors();
        Assert.assertEquals(3, properties.size());

        Assert.assertEquals(properties.get(0).getName(), StandardS3EncryptionService.ENCRYPTION_STRATEGY.getName());
        Assert.assertEquals(properties.get(1).getName(), StandardS3EncryptionService.ENCRYPTION_VALUE.getName());
        Assert.assertEquals(properties.get(2).getName(), StandardS3EncryptionService.KMS_REGION.getName());
    }
}
