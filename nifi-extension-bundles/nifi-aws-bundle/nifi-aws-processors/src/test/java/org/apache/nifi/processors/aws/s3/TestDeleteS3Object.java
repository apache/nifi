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
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDeleteS3Object {

    private TestRunner runner = null;
    private DeleteS3Object mockDeleteS3Object = null;
    private S3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = Mockito.mock(S3Client.class);
        Mockito.when(mockS3Client.utilities()).thenReturn(S3Utilities.builder().region(Region.US_WEST_2).build());
        mockDeleteS3Object = new DeleteS3Object() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockDeleteS3Object);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testDeleteObjectSimple() {
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<DeleteObjectRequest> captureRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteObject(captureRequest.capture());
        DeleteObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("delete-key", request.key());
    }

    @Test
    public void testDeleteObjectSimpleRegionFromFlowFileAttribute() {
        runner.setProperty(RegionUtil.REGION, "use-custom-region");
        runner.setProperty(RegionUtil.CUSTOM_REGION, "${s3.region}");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        attrs.put("s3.region", "us-east-1");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteObjectS3Exception() {
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(S3Exception.builder().message("NoSuchBucket").build()).when(mockS3Client).deleteObject(Mockito.any(DeleteObjectRequest.class));

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testDeleteVersionSimple() {
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(DeleteS3Object.VERSION_ID, "test-version");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<DeleteObjectRequest> captureRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteObject(captureRequest.capture());
        DeleteObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("test-key", request.key());
        assertEquals("test-version", request.versionId());
    }

    @Test
    public void testDeleteVersionFromExpressions() {
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(DeleteS3Object.VERSION_ID, "${s3.version}");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        attrs.put("s3.bucket", "test-bucket");
        attrs.put("s3.version", "test-version");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<DeleteObjectRequest> captureRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteObject(captureRequest.capture());
        DeleteObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("test-key", request.key());
        assertEquals("test-version", request.versionId());
    }

}
