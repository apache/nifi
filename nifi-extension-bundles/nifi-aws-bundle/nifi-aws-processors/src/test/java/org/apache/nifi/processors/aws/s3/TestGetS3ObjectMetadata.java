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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.s3.api.MetadataTarget;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestGetS3ObjectMetadata {
    private TestRunner runner;

    private S3Client mockS3Client;

    private HeadObjectResponse mockResponse;

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
        GetS3ObjectMetadata mockGetS3ObjectMetadata = new GetS3ObjectMetadata() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockGetS3ObjectMetadata);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");

        mockResponse = mock(HeadObjectResponse.class);
    }

    private void run() {
        runner.setProperty(GetS3ObjectMetadata.BUCKET_WITH_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(GetS3ObjectMetadata.KEY, "${filename}");
        runner.enqueue("", Map.of("s3.bucket", "test-data", "filename", "test.txt"));

        runner.run();
    }

    private Map<String, String> setupObjectMetadata() {
        Map<String, String> combined = new HashMap<>();

        long contentLength = 10;
        String contentLanguage = "EN";

        when(mockResponse.contentLength()).thenReturn(contentLength);
        when(mockResponse.contentLanguage()).thenReturn(contentLanguage);

        combined.put("Content-Length", String.valueOf(contentLength));
        combined.put("Content-Language", contentLanguage);

        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("user1", "a");
        userMetadata.put("user2", "b");
        userMetadata.put("mighthaveto", "excludemelater");

        when(mockResponse.metadata()).thenReturn(userMetadata);

        combined.putAll(userMetadata);

        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(mockResponse);

        return combined;
    }

    @DisplayName("Validate fetch metadata to attribute routes to found when the file exists")
    @Test
    void testFetchMetadataToAttributeExists() {
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, MetadataTarget.ATTRIBUTES);
        runner.setProperty(GetS3ObjectMetadata.ATTRIBUTE_INCLUDE_PATTERN, "");

        Map<String, String> combined = setupObjectMetadata();

        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FOUND, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetS3ObjectMetadata.REL_FOUND).getFirst();
        combined.forEach((k, v) -> {
            String key = String.format("s3.%s", k);
            String val = flowFile.getAttribute(key);
            assertEquals(v, val);
        });
    }

    @DisplayName("Validate attribution exclusion")
    @Test
    void testFetchMetadataToAttributeExclusion() {
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, MetadataTarget.ATTRIBUTES);
        runner.setProperty(GetS3ObjectMetadata.ATTRIBUTE_INCLUDE_PATTERN, "(Content|user)");

        Map<String, String> metadata = setupObjectMetadata();
        Map<String, String> musthave = new HashMap<>(metadata);
        musthave.remove("mighthaveto");

        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FOUND, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetS3ObjectMetadata.REL_FOUND).getFirst();
        musthave.forEach((k, v) -> {
            String key = String.format("s3.%s", k);
            String val = flowFile.getAttribute(key);
            assertEquals(v, val);
        });

        assertNull(flowFile.getAttribute("s3.mighthaveto"));
    }

    @DisplayName("Validate fetch to attribute mode routes to failure on S3 error")
    @Test
    void testFetchMetadataToAttributeS3Error() {
        AwsServiceException exception = S3Exception.builder().message("test").statusCode(501).build();

        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, MetadataTarget.ATTRIBUTES);
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FAILURE, 1);
    }

    @DisplayName("Validate fetch metadata to attribute routes to not-found when when the file doesn't exist")
    @Test
    void testFetchMetadataToAttributeNotExist() {
        AwsServiceException exception = S3Exception.builder().message("test").statusCode(404).build();

        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, MetadataTarget.ATTRIBUTES);
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_NOT_FOUND, 1);
    }

    @DisplayName("Validate fetch metadata to FlowFile body routes to found when the file exists")
    @Test
    void testFetchMetadataToBodyExists() {
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, MetadataTarget.FLOWFILE_BODY);
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(mockResponse);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FOUND, 1);
    }

    @DisplayName("Validate fetch metadata to FlowFile body routes to not-found when when the file doesn't exist")
    @Test
    void testFetchMetadataToBodyNotExist() {
        AwsServiceException exception = S3Exception.builder().message("test").statusCode(404).build();

        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, MetadataTarget.FLOWFILE_BODY);
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_NOT_FOUND, 1);
    }

    @DisplayName("Validate fetch to FlowFile body mode routes to failure on S3 error")
    @Test
    void testFetchMetadataToBodyS3Error() {
        AwsServiceException exception = S3Exception.builder().message("test").statusCode(501).build();

        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, MetadataTarget.FLOWFILE_BODY);
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FAILURE, 1);
    }
}
