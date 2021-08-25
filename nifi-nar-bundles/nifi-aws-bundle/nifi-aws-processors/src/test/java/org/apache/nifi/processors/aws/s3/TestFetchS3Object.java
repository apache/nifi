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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringInputStream;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestFetchS3Object {

    private TestRunner runner = null;
    private FetchS3Object mockFetchS3Object = null;
    private AmazonS3Client actualS3Client = null;
    private AmazonS3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        mockFetchS3Object = new FetchS3Object() {
            protected AmazonS3Client getClient() {
                actualS3Client = client;
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockFetchS3Object);
    }

    @Test
    public void testGetObject() throws IOException {
        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        S3Object s3ObjectResponse = new S3Object();
        s3ObjectResponse.setBucketName("response-bucket-name");
        s3ObjectResponse.setKey("response-key");
        s3ObjectResponse.setObjectContent(new StringInputStream("Some Content"));
        ObjectMetadata metadata = Mockito.spy(ObjectMetadata.class);
        metadata.setContentDisposition("key/path/to/file.txt");
        metadata.setContentType("text/plain");
        metadata.setContentMD5("testMD5hash");
        Date expiration = new Date();
        metadata.setExpirationTime(expiration);
        metadata.setExpirationTimeRuleId("testExpirationRuleId");
        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("userKey1", "userValue1");
        userMetadata.put("userKey2", "userValue2");
        metadata.setUserMetadata(userMetadata);
        metadata.setSSEAlgorithm("testAlgorithm");
        Mockito.when(metadata.getETag()).thenReturn("test-etag");
        s3ObjectResponse.setObjectMetadata(metadata);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run(1);

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        Assertions.assertEquals("request-bucket", request.getBucketName());
        Assertions.assertEquals("request-key", request.getKey());
        Assertions.assertFalse(request.isRequesterPays());
        Assertions.assertNull(request.getVersionId());

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeEquals("s3.bucket", "response-bucket-name");
        ff.assertAttributeEquals(CoreAttributes.FILENAME.key(), "file.txt");
        ff.assertAttributeEquals(CoreAttributes.PATH.key(), "key/path/to");
        ff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), "key/path/to/file.txt");
        ff.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        ff.assertAttributeEquals("hash.value", "testMD5hash");
        ff.assertAttributeEquals("hash.algorithm", "MD5");
        ff.assertAttributeEquals("s3.etag", "test-etag");
        ff.assertAttributeEquals("s3.expirationTime", String.valueOf(expiration.getTime()));
        ff.assertAttributeEquals("s3.expirationTimeRuleId", "testExpirationRuleId");
        ff.assertAttributeEquals("userKey1", "userValue1");
        ff.assertAttributeEquals("userKey2", "userValue2");
        ff.assertAttributeEquals("s3.sseAlgorithm", "testAlgorithm");
        ff.assertContentEquals("Some Content");
    }

    @Test
    public void testGetObjectWithRequesterPays() throws IOException {
        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        runner.setProperty(FetchS3Object.REQUESTER_PAYS, "true");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        S3Object s3ObjectResponse = new S3Object();
        s3ObjectResponse.setBucketName("response-bucket-name");
        s3ObjectResponse.setKey("response-key");
        s3ObjectResponse.setObjectContent(new StringInputStream("Some Content"));
        ObjectMetadata metadata = Mockito.spy(ObjectMetadata.class);
        metadata.setContentDisposition("key/path/to/file.txt");
        metadata.setContentType("text/plain");
        metadata.setContentMD5("testMD5hash");
        Date expiration = new Date();
        metadata.setExpirationTime(expiration);
        metadata.setExpirationTimeRuleId("testExpirationRuleId");
        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("userKey1", "userValue1");
        userMetadata.put("userKey2", "userValue2");
        metadata.setUserMetadata(userMetadata);
        metadata.setSSEAlgorithm("testAlgorithm");
        Mockito.when(metadata.getETag()).thenReturn("test-etag");
        s3ObjectResponse.setObjectMetadata(metadata);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run(1);

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        Assertions.assertEquals("request-bucket", request.getBucketName());
        Assertions.assertEquals("request-key", request.getKey());
        Assertions.assertTrue(request.isRequesterPays());
        Assertions.assertNull(request.getVersionId());

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeEquals("s3.bucket", "response-bucket-name");
        ff.assertAttributeEquals(CoreAttributes.FILENAME.key(), "file.txt");
        ff.assertAttributeEquals(CoreAttributes.PATH.key(), "key/path/to");
        ff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), "key/path/to/file.txt");
        ff.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        ff.assertAttributeEquals("hash.value", "testMD5hash");
        ff.assertAttributeEquals("hash.algorithm", "MD5");
        ff.assertAttributeEquals("s3.etag", "test-etag");
        ff.assertAttributeEquals("s3.expirationTime", String.valueOf(expiration.getTime()));
        ff.assertAttributeEquals("s3.expirationTimeRuleId", "testExpirationRuleId");
        ff.assertAttributeEquals("userKey1", "userValue1");
        ff.assertAttributeEquals("userKey2", "userValue2");
        ff.assertAttributeEquals("s3.sseAlgorithm", "testAlgorithm");
        ff.assertContentEquals("Some Content");
    }

    @Test
    public void testGetObjectVersion() throws IOException {
        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        runner.setProperty(FetchS3Object.VERSION_ID, "${s3.version}");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        attrs.put("s3.version", "request-version");
        runner.enqueue(new byte[0], attrs);

        S3Object s3ObjectResponse = new S3Object();
        s3ObjectResponse.setBucketName("response-bucket-name");
        s3ObjectResponse.setObjectContent(new StringInputStream("Some Content"));
        ObjectMetadata metadata = Mockito.spy(ObjectMetadata.class);
        metadata.setContentDisposition("key/path/to/file.txt");
        Mockito.when(metadata.getVersionId()).thenReturn("response-version");
        s3ObjectResponse.setObjectMetadata(metadata);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run(1);

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        Assertions.assertEquals("request-bucket", request.getBucketName());
        Assertions.assertEquals("request-key", request.getKey());
        Assertions.assertEquals("request-version", request.getVersionId());

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeEquals("s3.bucket", "response-bucket-name");
        ff.assertAttributeEquals(CoreAttributes.FILENAME.key(), "file.txt");
        ff.assertAttributeEquals(CoreAttributes.PATH.key(), "key/path/to");
        ff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), "key/path/to/file.txt");
        ff.assertAttributeEquals("s3.version", "response-version");
        ff.assertContentEquals("Some Content");
    }


    @Test
    public void testGetObjectExceptionGoesToFailure() throws IOException {
        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(new AmazonS3Exception("NoSuchBucket")).when(mockS3Client).getObject(Mockito.any());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testGetObjectReturnsNull() throws IOException {
        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.when(mockS3Client.getObject(Mockito.any())).thenReturn(null);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testFlowFileAccessExceptionGoesToFailure() throws IOException {
        runner.setProperty(FetchS3Object.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        AmazonS3Exception amazonException = new AmazonS3Exception("testing");
        Mockito.doThrow(new FlowFileAccessException("testing nested", amazonException)).when(mockS3Client).getObject(Mockito.any());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }
    @Test
    public void testGetPropertyDescriptors() throws Exception {
        FetchS3Object processor = new FetchS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        Assertions.assertEquals(21, pd.size(), "size should be eq");
        Assertions.assertTrue(pd.contains(FetchS3Object.ACCESS_KEY));
        Assertions.assertTrue(pd.contains(FetchS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE));
        Assertions.assertTrue(pd.contains(FetchS3Object.BUCKET));
        Assertions.assertTrue(pd.contains(FetchS3Object.CREDENTIALS_FILE));
        Assertions.assertTrue(pd.contains(FetchS3Object.ENDPOINT_OVERRIDE));
        Assertions.assertTrue(pd.contains(FetchS3Object.KEY));
        Assertions.assertTrue(pd.contains(FetchS3Object.REGION));
        Assertions.assertTrue(pd.contains(FetchS3Object.SECRET_KEY));
        Assertions.assertTrue(pd.contains(FetchS3Object.SIGNER_OVERRIDE));
        Assertions.assertTrue(pd.contains(FetchS3Object.SSL_CONTEXT_SERVICE));
        Assertions.assertTrue(pd.contains(FetchS3Object.TIMEOUT));
        Assertions.assertTrue(pd.contains(FetchS3Object.VERSION_ID));
        Assertions.assertTrue(pd.contains(FetchS3Object.ENCRYPTION_SERVICE));
        Assertions.assertTrue(pd.contains(FetchS3Object.PROXY_CONFIGURATION_SERVICE));
        Assertions.assertTrue(pd.contains(FetchS3Object.PROXY_HOST));
        Assertions.assertTrue(pd.contains(FetchS3Object.PROXY_HOST_PORT));
        Assertions.assertTrue(pd.contains(FetchS3Object.PROXY_USERNAME));
        Assertions.assertTrue(pd.contains(FetchS3Object.PROXY_PASSWORD));
        Assertions.assertTrue(pd.contains(FetchS3Object.REQUESTER_PAYS));

    }
}
