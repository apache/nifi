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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.util.RegionUtilV1;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestDeleteS3Object {

    private TestRunner runner = null;
    private DeleteS3Object mockDeleteS3Object = null;
    private AmazonS3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        mockDeleteS3Object = new DeleteS3Object() {
            @Override
            protected AmazonS3Client getS3Client(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockDeleteS3Object);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testDeleteObjectSimple() {
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<DeleteObjectRequest> captureRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteObject(captureRequest.capture());
        DeleteObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("delete-key", request.getKey());
        Mockito.verify(mockS3Client, Mockito.never()).deleteVersion(Mockito.any(DeleteVersionRequest.class));
    }

    @Test
    public void testDeleteObjectSimpleRegionFromFlowFileAttribute() {
        runner.setProperty(RegionUtilV1.S3_REGION, "attribute-defined-region");
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
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(new AmazonS3Exception("NoSuchBucket")).when(mockS3Client).deleteObject(Mockito.any());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
        Mockito.verify(mockS3Client, Mockito.never()).deleteVersion(Mockito.any(DeleteVersionRequest.class));
    }

    @Test
    public void testDeleteVersionSimple() {
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(DeleteS3Object.VERSION_ID, "test-version");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<DeleteVersionRequest> captureRequest = ArgumentCaptor.forClass(DeleteVersionRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteVersion(captureRequest.capture());
        DeleteVersionRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("test-key", request.getKey());
        assertEquals("test-version", request.getVersionId());
        Mockito.verify(mockS3Client, Mockito.never()).deleteObject(Mockito.any(DeleteObjectRequest.class));
    }

    @Test
    public void testDeleteVersionFromExpressions() {
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(DeleteS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(DeleteS3Object.VERSION_ID, "${s3.version}");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        attrs.put("s3.bucket", "test-bucket");
        attrs.put("s3.version", "test-version");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<DeleteVersionRequest> captureRequest = ArgumentCaptor.forClass(DeleteVersionRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).deleteVersion(captureRequest.capture());
        DeleteVersionRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("test-key", request.getKey());
        assertEquals("test-version", request.getVersionId());
        Mockito.verify(mockS3Client, Mockito.never()).deleteObject(Mockito.any(DeleteObjectRequest.class));
    }

}
