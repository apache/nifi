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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringInputStream;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestFetchS3Object {

    private TestRunner runner = null;
    private FetchS3Object mockFetchS3Object = null;
    private AmazonS3Client actualS3Client = null;
    private AmazonS3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(AmazonS3Client.class);
        mockFetchS3Object = new FetchS3Object() {
            @Override
            protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockFetchS3Object);
    }

    @Test
    public void testGetObject() throws IOException {
        runner.setProperty(FetchS3Object.S3_REGION, "attribute-defined-region");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        attrs.put("s3.region", "us-west-2");
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
        when(metadata.getETag()).thenReturn("test-etag");
        s3ObjectResponse.setObjectMetadata(metadata);
        when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        final long mockSize = 20L;
        final ObjectMetadata objectMetadata = mock(ObjectMetadata.class);
        when(objectMetadata.getContentLength()).thenReturn(mockSize);
        when(mockS3Client.getObjectMetadata(any())).thenReturn(objectMetadata);

        runner.run(1);

        final List<ConfigVerificationResult> results = mockFetchS3Object.verify(runner.getProcessContext(), runner.getLogger(), attrs);
        assertEquals(2, results.size());
        results.forEach(result -> assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome()));

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("request-bucket", request.getBucketName());
        assertEquals("request-key", request.getKey());
        assertFalse(request.isRequesterPays());
        assertNull(request.getVersionId());

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
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
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
        when(metadata.getETag()).thenReturn("test-etag");
        s3ObjectResponse.setObjectMetadata(metadata);
        when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run(1);

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("request-bucket", request.getBucketName());
        assertEquals("request-key", request.getKey());
        assertTrue(request.isRequesterPays());
        assertNull(request.getVersionId());

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
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
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
        when(metadata.getVersionId()).thenReturn("response-version");
        s3ObjectResponse.setObjectMetadata(metadata);
        when(mockS3Client.getObject(Mockito.any())).thenReturn(s3ObjectResponse);

        runner.run(1);

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("request-bucket", request.getBucketName());
        assertEquals("request-key", request.getKey());
        assertEquals("request-version", request.getVersionId());

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
    public void testGetObjectExceptionGoesToFailure() {
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(new AmazonS3Exception("NoSuchBucket")).when(mockS3Client).getObject(Mockito.any());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testFetchObject_FailAdditionalAttributesBucketName() {
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket-bad-name");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        final AmazonS3Exception exception = new AmazonS3Exception("The specified bucket does not exist");

        final Map<String, String> details = new LinkedHashMap<>();
        details.put("BucketName", "us-east-1");
        details.put("Error", "ABC123");

        exception.setAdditionalDetails(details);
        exception.setErrorCode("NoSuchBucket");
        exception.setStatusCode(HttpURLConnection.HTTP_NOT_FOUND);
        Mockito.doThrow(exception).when(mockS3Client).getObject(Mockito.any());
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchS3Object.REL_FAILURE);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.iterator().next();
        assertEquals("NoSuchBucket", flowFile.getAttribute("s3.errorCode"));
        assertTrue(exception.getMessage().startsWith(flowFile.getAttribute("s3.errorMessage")));
        assertEquals("404", flowFile.getAttribute("s3.statusCode"));
        assertEquals(exception.getClass().getName(), flowFile.getAttribute("s3.exception"));
    }

    @Test
    public void testFetchObject_FailAdditionalAttributesAuthentication() {
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket-bad-name");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        final AmazonS3Exception exception = new AmazonS3Exception("signature");
        exception.setAdditionalDetails(Collections.singletonMap("CanonicalRequestBytes", "AA BB CC DD EE FF"));
        exception.setErrorCode("SignatureDoesNotMatch");
        exception.setStatusCode(HttpURLConnection.HTTP_FORBIDDEN);
        Mockito.doThrow(exception).when(mockS3Client).getObject(Mockito.any());
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchS3Object.REL_FAILURE);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.iterator().next();
        assertEquals("SignatureDoesNotMatch", flowFile.getAttribute("s3.errorCode"));
        assertTrue(exception.getMessage().startsWith(flowFile.getAttribute("s3.errorMessage")));
        assertEquals("403", flowFile.getAttribute("s3.statusCode"));
        assertEquals(exception.getClass().getName(), flowFile.getAttribute("s3.exception"));
    }

    @Test
    public void testFetchObject_FailAdditionalAttributesNetworkFailure() {
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket-bad-name");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        final SdkClientException exception = new SdkClientException("message");
        Mockito.doThrow(exception).when(mockS3Client).getObject(Mockito.any());
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchS3Object.REL_FAILURE);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.iterator().next();
        assertEquals(exception.getClass().getName(), flowFile.getAttribute("s3.exception"));
    }

    @Test
    public void testGetObjectReturnsNull() {
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);
        when(mockS3Client.getObject(Mockito.any())).thenReturn(null);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testFlowFileAccessExceptionGoesToFailure() {
        runner.setProperty(FetchS3Object.S3_REGION, "us-east-1");
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
    public void testGetPropertyDescriptors() {
        FetchS3Object processor = new FetchS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals(23, pd.size(), "size should be eq");
        assertTrue(pd.contains(FetchS3Object.ACCESS_KEY));
        assertTrue(pd.contains(FetchS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(FetchS3Object.BUCKET));
        assertTrue(pd.contains(FetchS3Object.CREDENTIALS_FILE));
        assertTrue(pd.contains(FetchS3Object.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(FetchS3Object.KEY));
        assertTrue(pd.contains(FetchS3Object.S3_REGION));
        assertTrue(pd.contains(FetchS3Object.SECRET_KEY));
        assertTrue(pd.contains(FetchS3Object.SIGNER_OVERRIDE));
        assertTrue(pd.contains(FetchS3Object.S3_CUSTOM_SIGNER_CLASS_NAME));
        assertTrue(pd.contains(FetchS3Object.S3_CUSTOM_SIGNER_MODULE_LOCATION));
        assertTrue(pd.contains(FetchS3Object.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(FetchS3Object.TIMEOUT));
        assertTrue(pd.contains(FetchS3Object.VERSION_ID));
        assertTrue(pd.contains(FetchS3Object.ENCRYPTION_SERVICE));
        assertTrue(pd.contains(FetchS3Object.PROXY_CONFIGURATION_SERVICE));
        assertTrue(pd.contains(FetchS3Object.PROXY_HOST));
        assertTrue(pd.contains(FetchS3Object.PROXY_HOST_PORT));
        assertTrue(pd.contains(FetchS3Object.PROXY_USERNAME));
        assertTrue(pd.contains(FetchS3Object.PROXY_PASSWORD));
        assertTrue(pd.contains(FetchS3Object.REQUESTER_PAYS));

    }
}
