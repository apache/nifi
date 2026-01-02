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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.utils.StringInputStream;

import java.net.HttpURLConnection;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFetchS3Object {

    private TestRunner runner = null;
    private FetchS3Object mockFetchS3Object = null;
    private S3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(S3Client.class);
        Mockito.when(mockS3Client.utilities()).thenReturn(S3Utilities.builder().region(Region.US_WEST_2).build());
        mockFetchS3Object = new FetchS3Object() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }

            @Override
            protected S3Client createClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockFetchS3Object);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testGetObject() {
        runner.setProperty(RegionUtil.REGION, "use-custom-region");
        runner.setProperty(RegionUtil.CUSTOM_REGION, "${s3.region}");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        attrs.put("s3.region", "us-west-2");
        runner.enqueue(new byte[0], attrs);

        Instant expirationTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String expirationTimeRfc1123 = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.ofInstant(expirationTime, ZoneOffset.UTC));

        GetObjectResponse response = GetObjectResponse.builder()
                .eTag("test-etag")
                .contentLength(12L)
                .contentDisposition("key/path/to/file.txt")
                .contentType("text/plain")
                .checksumCRC32("testCRC32hash")
                .expiration(String.format("expiry-date=\"%s\", rule-id=\"%s\"", expirationTimeRfc1123, "testExpirationRuleId"))
                .serverSideEncryption(ServerSideEncryption.AES256)
                .metadata(Map.of("userKey1", "userValue1", "userKey2", "userValue2"))
                .build();

        ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(response, AbortableInputStream.create(new StringInputStream("Some Content")));
        when(mockS3Client.getObject(Mockito.any(GetObjectRequest.class))).thenReturn(responseStream);

        HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
        when(mockS3Client.headObject(Mockito.any(HeadObjectRequest.class))).thenReturn(headResponse);

        runner.run(1);

        final List<ConfigVerificationResult> results = mockFetchS3Object.verify(runner.getProcessContext(), runner.getLogger(), attrs);
        assertEquals(2, results.size());
        results.forEach(result -> assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome()));

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("test-key", request.key());
        assertNull(request.requestPayer());
        assertNull(request.versionId());

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeEquals("s3.bucket", "test-bucket");
        ff.assertAttributeEquals(CoreAttributes.FILENAME.key(), "file.txt");
        ff.assertAttributeEquals(CoreAttributes.PATH.key(), "key/path/to");
        ff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), "key/path/to/file.txt");
        ff.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        ff.assertAttributeEquals("hash.value", "testCRC32hash");
        ff.assertAttributeEquals("hash.algorithm", "CRC32");
        ff.assertAttributeEquals("s3.etag", "test-etag");
        ff.assertAttributeEquals("s3.expirationTime", String.valueOf(expirationTime.toEpochMilli()));
        ff.assertAttributeEquals("s3.expirationTimeRuleId", "testExpirationRuleId");
        ff.assertAttributeEquals("userKey1", "userValue1");
        ff.assertAttributeEquals("userKey2", "userValue2");
        ff.assertAttributeEquals("s3.sseAlgorithm", "AES256");
        ff.assertContentEquals("Some Content");
    }

    @Test
    public void testGetObjectWithRequesterPays() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(FetchS3Object.REQUESTER_PAYS, "true");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        runner.enqueue(new byte[0], attrs);

        Instant expirationTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String expirationTimeRfc1123 = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.ofInstant(expirationTime, ZoneOffset.UTC));

        GetObjectResponse response = GetObjectResponse.builder()
                .eTag("test-etag")
                .contentLength(12L)
                .contentDisposition("key/path/to/file.txt")
                .contentType("text/plain")
                .checksumCRC32("testCRC32hash")
                .expiration(String.format("expiry-date=\"%s\", rule-id=\"%s\"", expirationTimeRfc1123, "testExpirationRuleId"))
                .serverSideEncryption(ServerSideEncryption.AES256)
                .metadata(Map.of("userKey1", "userValue1", "userKey2", "userValue2"))
                .build();

        ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(response, AbortableInputStream.create(new StringInputStream("Some Content")));
        when(mockS3Client.getObject(Mockito.any(GetObjectRequest.class))).thenReturn(responseStream);

        runner.run(1);

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("test-key", request.key());
        assertEquals(RequestPayer.REQUESTER, request.requestPayer());
        assertNull(request.versionId());

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeEquals("s3.bucket", "test-bucket");
        ff.assertAttributeEquals(CoreAttributes.FILENAME.key(), "file.txt");
        ff.assertAttributeEquals(CoreAttributes.PATH.key(), "key/path/to");
        ff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), "key/path/to/file.txt");
        ff.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
        ff.assertAttributeEquals("hash.value", "testCRC32hash");
        ff.assertAttributeEquals("hash.algorithm", "CRC32");
        ff.assertAttributeEquals("s3.etag", "test-etag");
        ff.assertAttributeEquals("s3.expirationTime", String.valueOf(expirationTime.toEpochMilli()));
        ff.assertAttributeEquals("s3.expirationTimeRuleId", "testExpirationRuleId");
        ff.assertAttributeEquals("userKey1", "userValue1");
        ff.assertAttributeEquals("userKey2", "userValue2");
        ff.assertAttributeEquals("s3.sseAlgorithm", "AES256");
        ff.assertContentEquals("Some Content");
    }

    @Test
    public void testGetObjectVersion() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(FetchS3Object.VERSION_ID, "${s3.version}");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-key");
        attrs.put("s3.version", "test-version");
        runner.enqueue(new byte[0], attrs);

        GetObjectResponse response = GetObjectResponse.builder()
                .versionId("test-version")
                .eTag("test-etag")
                .contentLength(12L)
                .contentDisposition("key/path/to/file.txt")
                .build();

        ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(response, AbortableInputStream.create(new StringInputStream("Some Content")));
        when(mockS3Client.getObject(Mockito.any(GetObjectRequest.class))).thenReturn(responseStream);

        runner.run(1);

        ArgumentCaptor<GetObjectRequest> captureRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObject(captureRequest.capture());
        GetObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("test-key", request.key());
        assertEquals("test-version", request.versionId());

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeEquals("s3.bucket", "test-bucket");
        ff.assertAttributeEquals(CoreAttributes.FILENAME.key(), "file.txt");
        ff.assertAttributeEquals(CoreAttributes.PATH.key(), "key/path/to");
        ff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), "key/path/to/file.txt");
        ff.assertAttributeEquals("s3.version", "test-version");
        ff.assertContentEquals("Some Content");
    }


    @Test
    public void testGetObjectExceptionGoesToFailure() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(S3Exception.builder().message("NoSuchBucket").build()).when(mockS3Client).getObject(Mockito.any(GetObjectRequest.class));

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testFetchObject_FailAdditionalAttributesBucketName() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "request-bucket-bad-name");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        final AwsServiceException exception = buildS3Exception(HttpURLConnection.HTTP_NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist",
                Map.of("BucketName", List.of("us-east-1"), "Error", List.of("ABC123")));
        Mockito.doThrow(exception).when(mockS3Client).getObject(Mockito.any(GetObjectRequest.class));
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchS3Object.REL_FAILURE);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.iterator().next();
        assertEquals("NoSuchBucket", flowFile.getAttribute("s3.errorCode"));
        assertEquals("The specified bucket does not exist", flowFile.getAttribute("s3.errorMessage"));
        assertEquals("404", flowFile.getAttribute("s3.statusCode"));
        assertEquals(exception.getClass().getName(), flowFile.getAttribute("s3.exception"));
    }

    @Test
    public void testFetchObject_FailAdditionalAttributesAuthentication() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "request-bucket-bad-name");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        final AwsServiceException exception = buildS3Exception(HttpURLConnection.HTTP_FORBIDDEN, "SignatureDoesNotMatch", "signature",
                Map.of("CanonicalRequestBytes", List.of("AA BB CC DD EE FF")));
        Mockito.doThrow(exception).when(mockS3Client).getObject(Mockito.any(GetObjectRequest.class));
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchS3Object.REL_FAILURE);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.iterator().next();
        assertEquals("SignatureDoesNotMatch", flowFile.getAttribute("s3.errorCode"));
        assertEquals("signature", flowFile.getAttribute("s3.errorMessage"));
        assertEquals("403", flowFile.getAttribute("s3.statusCode"));
        assertEquals(exception.getClass().getName(), flowFile.getAttribute("s3.exception"));
    }

    @Test
    public void testFetchObject_FailAdditionalAttributesNetworkFailure() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "request-bucket-bad-name");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        final SdkClientException exception = SdkClientException.builder().message("message").build();
        Mockito.doThrow(exception).when(mockS3Client).getObject(Mockito.any(GetObjectRequest.class));
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchS3Object.REL_FAILURE);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.iterator().next();
        assertEquals(exception.getClass().getName(), flowFile.getAttribute("s3.exception"));
    }

    @Test
    public void testGetObjectReturnsNull() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);
        when(mockS3Client.getObject(Mockito.any(GetObjectRequest.class))).thenReturn(null);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testFlowFileAccessExceptionGoesToFailure() {
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "request-bucket");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "request-key");
        runner.enqueue(new byte[0], attrs);

        AwsServiceException amazonException = S3Exception.builder().message("testing").build();
        Mockito.doThrow(new FlowFileAccessException("testing nested", amazonException)).when(mockS3Client).getObject(Mockito.any(GetObjectRequest.class));

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    void testMigration() {
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed =
                Map.of("encryption-service", AbstractS3Processor.ENCRYPTION_SERVICE.getName(),
                        "requester-pays", FetchS3Object.REQUESTER_PAYS.getName(),
                        "range-start", FetchS3Object.RANGE_START.getName(),
                        "range-length", FetchS3Object.RANGE_LENGTH.getName());

        expectedRenamed.forEach((key, value) -> assertEquals(value, propertyMigrationResult.getPropertiesRenamed().get(key)));
    }

    private AwsServiceException buildS3Exception(int statusCode, String errorCode, String errorMessage, Map<String, List<String>> headers) {
        return S3Exception.builder()
                .statusCode(statusCode)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode)
                        .errorMessage(errorMessage)
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .headers(headers)
                                .build())
                        .build())
                .build();

    }

}
