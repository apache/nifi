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

package org.apache.nifi.processors.aws.s3.service;

import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.util.Map;

import static org.apache.nifi.processors.aws.AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3FileResourceServiceTest {
    private static final String CONTROLLER_SERVICE = "AWSCredentialsService";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY = "key";
    private static final long CONTENT_LENGTH = 10L;

    @Mock
    private S3Client client;

    @Mock
    private ResponseInputStream<GetObjectResponse> responseStream;

    @Mock
    private GetObjectResponse response;

    @InjectMocks
    private TestS3FileResourceService service;
    private TestRunner runner;

    private GetObjectRequest request;

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("S3FileResourceService", service);

        request = GetObjectRequest.builder().bucket(BUCKET_NAME).key(KEY).build();
    }

    @Test
    void testGetFileResourceHappyPath() throws InitializationException {
        setupS3Client();
        setupService();

        FileResource fileResource = service.getFileResource(Map.of());
        assertFileResource(fileResource);
    }

    @Test
    void testNonExistingObject() throws InitializationException {
        when(client.getObject(request)).thenThrow(NoSuchKeyException.builder().build());
        setupService();

        assertThrows(ProcessException.class, () -> service.getFileResource(Map.of()), "Failed to fetch s3 object");
        verify(client).getObject(request);
        verifyNoMoreInteractions(client);
    }

    @Test
    void testValidBlobUsingELButMissingAttribute() throws InitializationException {
        setupService("${s3.bucket}", "${key}");

        assertThrows(ProcessException.class,
                () -> service.getFileResource(Map.of()), "Bucket name or key value is missing");
        verifyNoInteractions(client);
    }

    @Test
    void testValidBlobUsingEL() throws InitializationException {
        String bucketProperty = "s3.bucket";
        String keyProperty = "key";
        setupService("${" + bucketProperty + "}", "${" + keyProperty + "}");
        setupS3Client();

        FileResource fileResource = service.getFileResource(Map.of(
                bucketProperty, BUCKET_NAME,
                keyProperty, KEY));
        assertFileResource(fileResource);
    }

    private void assertFileResource(FileResource fileResource) {
        assertNotNull(fileResource);
        assertEquals(fileResource.getInputStream(), responseStream);
        assertEquals(fileResource.getSize(), CONTENT_LENGTH);
        verify(client).getObject(request);
        verify(responseStream).response();
        verify(response).contentLength();
    }

    private void setupService() throws InitializationException {
        setupService(BUCKET_NAME, KEY);
    }

    private void setupService(String bucket, String key) throws InitializationException {
        final AwsCredentialsProviderService credentialsService = new AWSCredentialsProviderControllerService();

        runner.addControllerService(CONTROLLER_SERVICE, credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(service, AWS_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(service, S3FileResourceService.KEY, key);
        runner.setProperty(service, S3FileResourceService.BUCKET_WITH_DEFAULT_VALUE, bucket);

        runner.enableControllerService(service);
    }

    private void setupS3Client() {
        when(client.getObject(request)).thenReturn(responseStream);
        when(responseStream.response()).thenReturn(response);
        when(response.contentLength()).thenReturn(CONTENT_LENGTH);
    }

    private static class TestS3FileResourceService extends S3FileResourceService {

        private final S3Client client;

        private TestS3FileResourceService(S3Client client) {
            this.client = client;
        }

        @Override
        protected S3Client getS3Client(Map<String, String> attributes, AwsCredentialsProvider credentialsProvider) {
            return client;
        }
    }
}
