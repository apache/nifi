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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestPutS3Object {
    private TestRunner runner = null;
    private PutS3Object mockPutS3Object = null;
    private AmazonS3Client actualS3Client = null;
    private AmazonS3Client mockS3Client = null;

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        mockPutS3Object = new PutS3Object() {
            protected AmazonS3Client getClient() {
                actualS3Client = client;
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockPutS3Object);
    }

    @Test
    public void testPutSinglePart() {
        runner.setProperty(PutS3Object.REGION, "ap-northeast-1");
        runner.setProperty(PutS3Object.BUCKET, "test-bucket");
        runner.setProperty("x-custom-prop", "hello");
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "testfile.txt");
        runner.enqueue("Test Content", ffAttributes);

        PutObjectResult putObjectResult = Mockito.spy(PutObjectResult.class);
        Date expiration = new Date();
        putObjectResult.setExpirationTime(expiration);
        putObjectResult.setMetadata(new ObjectMetadata());
        putObjectResult.setVersionId("test-version");
        Mockito.when(putObjectResult.getETag()).thenReturn("test-etag");
        Mockito.when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class))).thenReturn(putObjectResult);
        MultipartUploadListing uploadListing = new MultipartUploadListing();
        Mockito.when(mockS3Client.listMultipartUploads(Mockito.any(ListMultipartUploadsRequest.class))).thenReturn(uploadListing);
        Mockito.when(mockS3Client.getResourceUrl(Mockito.anyString(), Mockito.anyString())).thenReturn("test-s3-url");

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
        PutObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(CoreAttributes.FILENAME.key(), "testfile.txt");
        ff0.assertAttributeEquals(PutS3Object.S3_ETAG_ATTR_KEY, "test-etag");
        ff0.assertAttributeEquals(PutS3Object.S3_VERSION_ATTR_KEY, "test-version");
    }

    @Test
    public void testPutSinglePartException() {
        runner.setProperty(PutS3Object.REGION, "ap-northeast-1");
        runner.setProperty(PutS3Object.BUCKET, "test-bucket");
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "testfile.txt");
        runner.enqueue("Test Content", ffAttributes);

        MultipartUploadListing uploadListing = new MultipartUploadListing();
        Mockito.when(mockS3Client.listMultipartUploads(Mockito.any(ListMultipartUploadsRequest.class))).thenReturn(uploadListing);
        Mockito.when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class))).thenThrow(new AmazonS3Exception("TestFail"));

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testSignerOverrideOptions() {
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final ClientConfiguration config = new ClientConfiguration();
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final List<AllowableValue> allowableSignerValues = PutS3Object.SIGNER_OVERRIDE.getAllowableValues();
        final String defaultSignerValue = PutS3Object.SIGNER_OVERRIDE.getDefaultValue();

        for (AllowableValue allowableSignerValue : allowableSignerValues) {
            String signerType = allowableSignerValue.getValue();
            if (!signerType.equals(defaultSignerValue)) {
                runner.setProperty(PutS3Object.SIGNER_OVERRIDE, signerType);
                ProcessContext context = runner.getProcessContext();
                try {
                    AmazonS3Client s3Client = processor.createClient(context, credentialsProvider, config);
                } catch (IllegalArgumentException argEx) {
                    Assert.fail(argEx.getMessage());
                }
            }
        }
    }

    @Test
    public void testGetPropertyDescriptors() throws Exception {
        PutS3Object processor = new PutS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 28, pd.size());
        assertTrue(pd.contains(PutS3Object.ACCESS_KEY));
        assertTrue(pd.contains(PutS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(PutS3Object.BUCKET));
        assertTrue(pd.contains(PutS3Object.CANNED_ACL));
        assertTrue(pd.contains(PutS3Object.CREDENTIALS_FILE));
        assertTrue(pd.contains(PutS3Object.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(PutS3Object.FULL_CONTROL_USER_LIST));
        assertTrue(pd.contains(PutS3Object.KEY));
        assertTrue(pd.contains(PutS3Object.OWNER));
        assertTrue(pd.contains(PutS3Object.READ_ACL_LIST));
        assertTrue(pd.contains(PutS3Object.READ_USER_LIST));
        assertTrue(pd.contains(PutS3Object.REGION));
        assertTrue(pd.contains(PutS3Object.SECRET_KEY));
        assertTrue(pd.contains(PutS3Object.SIGNER_OVERRIDE));
        assertTrue(pd.contains(PutS3Object.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(PutS3Object.TIMEOUT));
        assertTrue(pd.contains(PutS3Object.EXPIRATION_RULE_ID));
        assertTrue(pd.contains(PutS3Object.STORAGE_CLASS));
        assertTrue(pd.contains(PutS3Object.WRITE_ACL_LIST));
        assertTrue(pd.contains(PutS3Object.WRITE_USER_LIST));
        assertTrue(pd.contains(PutS3Object.SERVER_SIDE_ENCRYPTION));
    }

}
