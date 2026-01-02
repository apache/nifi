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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.aws.AbstractAwsProcessor;
import org.apache.nifi.processors.aws.ObsoleteAbstractAwsProcessorProperties;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestListS3 {

    private static final long TEST_TIMESTAMP = 1234567890;

    private TestRunner runner;
    private ListS3 listS3;
    private S3Client mockS3Client;
    private MockStateManager mockStateManager;
    private DistributedMapCacheClient mockCache;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(S3Client.class);
        listS3 = new ListS3() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }

            @Override
            protected S3Client createClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(listS3);
        mockStateManager = runner.getStateManager();
        mockCache = mock(DistributedMapCacheClient.class);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }


    @Test
    public void testList() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");

        Instant lastModified = Instant.now();
        S3Object s3Object1 = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("b/c")
                .lastModified(lastModified)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("d/e")
                .lastModified(lastModified)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertNull(request.requestPayer());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        ff0.assertAttributeEquals("s3.region", "eu-west-1");
        String lastModifiedTimestamp = String.valueOf(lastModified.toEpochMilli());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);

        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor())
                .verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("finding 3 total object(s)"));
    }

    @Test
    public void testListWithRecords() throws InitializationException {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");

        final MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.setProperty(ListS3.RECORD_WRITER, "record-writer");

        Instant lastModified = Instant.now();
        S3Object s3Object1 = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .size(0L)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("b/c")
                .lastModified(lastModified)
                .size(0L)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("d/e")
                .lastModified(lastModified)
                .size(0L)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertNull(request.requestPayer());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "3");
        flowFile.assertAttributeEquals("s3.region", "eu-west-1");
        flowFile.assertContentEquals("a,test-bucket,,," + lastModified.toEpochMilli() + ",0,,true,,,\n"
            + "b/c,test-bucket,,," + lastModified.toEpochMilli() + ",0,,true,,,\n"
            + "d/e,test-bucket,,," + lastModified.toEpochMilli() + ",0,,true,,,\n");
    }

    @Test
    public void testListWithRequesterPays() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.REQUESTER_PAYS, "true");

        Instant lastModified = Instant.now();
        S3Object s3Object1 = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("b/c")
                .lastModified(lastModified)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("d/e")
                .lastModified(lastModified)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals(RequestPayer.REQUESTER, request.requestPayer());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModified.toEpochMilli());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListWithRequesterPays_invalid() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.USE_VERSIONS, "true"); // requester pays cannot be used with versions
        runner.setProperty(ListS3.REQUESTER_PAYS, "true");

        runner.assertNotValid();
    }

    @Test
    public void testListVersion2() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.LIST_TYPE, "2");

        Instant lastModified = Instant.now();
        S3Object s3Object1 = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("b/c")
                .lastModified(lastModified)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("d/e")
                .lastModified(lastModified)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectsV2Request> captureRequest = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(mockS3Client, Mockito.times(1)).listObjectsV2(captureRequest.capture());
        ListObjectsV2Request request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertNull(request.requestPayer());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModified.toEpochMilli());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListVersion2WithRequesterPays() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.REQUESTER_PAYS, "true");
        runner.setProperty(ListS3.LIST_TYPE, "2");

        Instant lastModified = Instant.now();

        S3Object s3Object1 = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("b/c")
                .lastModified(lastModified)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("d/e")
                .lastModified(lastModified)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectsV2Request> captureRequest = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(mockS3Client, Mockito.times(1)).listObjectsV2(captureRequest.capture());
        ListObjectsV2Request request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals(RequestPayer.REQUESTER, request.requestPayer());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModified.toEpochMilli());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListVersions() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.USE_VERSIONS, "true");

        Instant lastModified = Instant.now();
        ObjectVersion objectVersion1 = ObjectVersion.builder()
                .key("test-key")
                .versionId("1")
                .lastModified(lastModified)
                .build();
        ObjectVersion objectVersion2 = ObjectVersion.builder()
                .key("test-key")
                .versionId("2")
                .lastModified(lastModified)
                .build();
        ListObjectVersionsResponse response = ListObjectVersionsResponse.builder()
                .versions(objectVersion1, objectVersion2)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjectVersions(any(ListObjectVersionsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectVersionsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectVersionsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjectVersions(captureRequest.capture());
        ListObjectVersionsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        verify(mockS3Client, Mockito.never()).listObjects(any(ListObjectsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "test-key");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        ff0.assertAttributeEquals("s3.lastModified", String.valueOf(lastModified.toEpochMilli()));
        ff0.assertAttributeEquals("s3.version", "1");
        MockFlowFile ff1 = flowFiles.get(1);
        ff1.assertAttributeEquals("filename", "test-key");
        ff1.assertAttributeEquals("s3.bucket", "test-bucket");
        ff1.assertAttributeEquals("s3.lastModified", String.valueOf(lastModified.toEpochMilli()));
        ff1.assertAttributeEquals("s3.version", "2");
    }

    @Test
    public void testListObjectsNothingNew() throws IOException {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");

        Calendar calendar = Calendar.getInstance();
        calendar.set(2017, Calendar.JUNE, 2);
        Instant objectLastModified = calendar.toInstant();
        long stateCurrentTimestamp = objectLastModified.toEpochMilli();

        Map<String, String> state = new HashMap<>();
        state.put(ListS3.CURRENT_TIMESTAMP, String.valueOf(stateCurrentTimestamp));
        state.put(ListS3.CURRENT_KEY_PREFIX + "0", "test-key");
        MockStateManager mockStateManager = runner.getStateManager();
        mockStateManager.setState(state, Scope.CLUSTER);

        S3Object s3Object = S3Object.builder()
                .key("test-key")
                .lastModified(objectLastModified)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 0);
    }


    @Test
    public void testListIgnoreByMinAge() throws IOException {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.MIN_AGE, "30 sec");

        Instant lastModifiedNow = Instant.now();
        Instant lastModifiedMinus1Hour = lastModifiedNow.minus(1, ChronoUnit.HOURS);
        Instant lastModifiedMinus3Hour = lastModifiedNow.minus(3, ChronoUnit.HOURS);
        S3Object s3Object1 = S3Object.builder()
                .key("minus-3hour")
                .lastModified(lastModifiedMinus3Hour)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("minus-1hour")
                .lastModified(lastModifiedMinus1Hour)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("now")
                .lastModified(lastModifiedNow)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        Map<String, String> stateMap = new HashMap<>();
        String previousTimestamp = String.valueOf(lastModifiedMinus3Hour.toEpochMilli());
        stateMap.put(ListS3.CURRENT_TIMESTAMP, previousTimestamp);
        stateMap.put(ListS3.CURRENT_KEY_PREFIX + "0", "minus-3hour");
        runner.getStateManager().setState(stateMap, Scope.CLUSTER);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "minus-1hour");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModifiedMinus1Hour.toEpochMilli());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListIgnoreByMaxAge() throws IOException {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.MAX_AGE, "30 sec");
        Instant lastModifiedNow = Instant.now();
        Instant lastModifiedMinus1Hour = lastModifiedNow.minus(1, ChronoUnit.HOURS);
        Instant lastModifiedMinus3Hour = lastModifiedNow.minus(3, ChronoUnit.HOURS);
        S3Object s3Object1 = S3Object.builder()
                .key("minus-3hour")
                .lastModified(lastModifiedMinus3Hour)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("minus-1hour")
                .lastModified(lastModifiedMinus1Hour)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("now")
                .lastModified(lastModifiedNow)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        Map<String, String> stateMap = new HashMap<>();
        String previousTimestamp = String.valueOf(lastModifiedMinus3Hour.toEpochMilli());
        stateMap.put(ListS3.CURRENT_TIMESTAMP, previousTimestamp);
        stateMap.put(ListS3.CURRENT_KEY_PREFIX + "0", "minus-3hour");
        runner.getStateManager().setState(stateMap, Scope.CLUSTER);
        runner.run();
        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "now");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModifiedNow.toEpochMilli());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testWriteObjectTags() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.WRITE_OBJECT_TAGS, "true");

        Instant lastModified = Instant.now();
        S3Object s3Object = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<GetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(GetObjectTaggingRequest.class);
        verify(mockS3Client, Mockito.times(1)).getObjectTagging(captureRequest.capture());
        GetObjectTaggingRequest request = captureRequest.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals("a", request.key());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));
    }

    @Test
    public void testWriteUserMetadata() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.WRITE_USER_METADATA, "true");

        Instant lastModified = Instant.now();
        S3Object s3Object = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<HeadObjectRequest> captureRequest = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(mockS3Client, Mockito.times(1)).headObject(captureRequest.capture());
        HeadObjectRequest request = captureRequest.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals("a", request.key());

        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));
    }

    @Test
    public void testNoTrackingList() {
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.NO_TRACKING);

        Instant lastModified = Instant.now();
        S3Object s3Object1 = S3Object.builder()
                .key("a")
                .lastModified(lastModified)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("b/c")
                .lastModified(lastModified)
                .build();
        S3Object s3Object3 = S3Object.builder()
                .key("d/e")
                .lastModified(lastModified)
                .build();
        ListObjectsResponse response = ListObjectsResponse.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertNull(request.requestPayer());
        verify(mockS3Client, Mockito.never()).listObjectVersions(any(ListObjectVersionsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        ff0.assertAttributeEquals("s3.region", "eu-west-1");
        String lastModifiedTimestamp = String.valueOf(lastModified.toEpochMilli());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");

        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor())
                .verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("finding 3"));

        runner.clearTransferState();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, null, Scope.CLUSTER);
    }

    @Test
    void testResetTimestampTrackingWhenBucketModified() throws Exception {
        setUpResetTrackingTest(ListS3.BY_TIMESTAMPS);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertEquals(TEST_TIMESTAMP, listS3.getListingSnapshot().getTimestamp());

        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "otherBucket");

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertEquals(0, listS3.getListingSnapshot().getTimestamp());
        mockStateManager.assertStateEquals(ListS3.CURRENT_TIMESTAMP, "0", Scope.CLUSTER);

        assertFalse(listS3.isResetTracking());
    }
    @Test
    void testResetTimestampTrackingWhenRegionModified() throws Exception {
        setUpResetTrackingTest(ListS3.BY_TIMESTAMPS);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertEquals(TEST_TIMESTAMP, listS3.getListingSnapshot().getTimestamp());

        runner.setProperty(RegionUtil.REGION, Region.EU_CENTRAL_1.id());

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertEquals(0, listS3.getListingSnapshot().getTimestamp());
        mockStateManager.assertStateEquals(ListS3.CURRENT_TIMESTAMP, "0", Scope.CLUSTER);

        assertFalse(listS3.isResetTracking());
    }

    @Test
    void testResetTimestampTrackingWhenPrefixModified() throws Exception {
        setUpResetTrackingTest(ListS3.BY_TIMESTAMPS);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertEquals(TEST_TIMESTAMP, listS3.getListingSnapshot().getTimestamp());

        runner.setProperty(ListS3.PREFIX, "prefix2");

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertEquals(0, listS3.getListingSnapshot().getTimestamp());
        mockStateManager.assertStateEquals(ListS3.CURRENT_TIMESTAMP, "0", Scope.CLUSTER);

        assertFalse(listS3.isResetTracking());
    }

    @Test
    void testResetTimestampTrackingWhenStrategyModified() throws Exception {
        setUpResetTrackingTest(ListS3.BY_TIMESTAMPS);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertEquals(TEST_TIMESTAMP, listS3.getListingSnapshot().getTimestamp());

        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.NO_TRACKING);

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertEquals(0, listS3.getListingSnapshot().getTimestamp());
        mockStateManager.assertStateNotSet(ListS3.CURRENT_TIMESTAMP, Scope.CLUSTER);

        assertFalse(listS3.isResetTracking());
    }

    @Test
    void testResetEntityTrackingWhenBucketModified() throws Exception {
        setUpResetTrackingTest(ListS3.BY_ENTITIES);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertNotNull(listS3.getListedEntityTracker());

        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "otherBucket");

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertNotNull(listS3.getListedEntityTracker());
        verify(mockCache).remove(eq("ListedEntities::" + listS3.getIdentifier()), any());

        assertFalse(listS3.isResetTracking());
    }

    @Test
    void testResetEntityTrackingWhenRegionModified() throws Exception {

        setUpResetTrackingTest(ListS3.BY_ENTITIES);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertNotNull(listS3.getListedEntityTracker());

        runner.setProperty(RegionUtil.REGION, Region.EU_CENTRAL_1.id());

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertNotNull(listS3.getListedEntityTracker());
        verify(mockCache).remove(eq("ListedEntities::" + listS3.getIdentifier()), any());

        assertFalse(listS3.isResetTracking());
    }

    @Test
    void testResetEntityTrackingWhenPrefixModified() throws Exception {
        setUpResetTrackingTest(ListS3.BY_ENTITIES);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertNotNull(listS3.getListedEntityTracker());

        runner.setProperty(ListS3.PREFIX, "prefix2");

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertNotNull(listS3.getListedEntityTracker());
        verify(mockCache).remove(eq("ListedEntities::" + listS3.getIdentifier()), any());

        assertFalse(listS3.isResetTracking());
    }

    @Test
    void testResetEntityTrackingWhenStrategyModified() throws Exception {
        setUpResetTrackingTest(ListS3.BY_ENTITIES);

        assertFalse(listS3.isResetTracking());

        runner.run();

        assertNotNull(listS3.getListedEntityTracker());

        runner.setProperty(ListS3.LISTING_STRATEGY, ListS3.NO_TRACKING);

        assertTrue(listS3.isResetTracking());

        runner.run();

        assertNull(listS3.getListedEntityTracker());
        verify(mockCache).remove(eq("ListedEntities::" + listS3.getIdentifier()), any());

        assertFalse(listS3.isResetTracking());
    }

    private void setUpResetTrackingTest(AllowableValue listingStrategy) throws Exception {
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(ListS3.LISTING_STRATEGY, listingStrategy);
        runner.setProperty(ListS3.PREFIX, "prefix1");

        if (listingStrategy == ListS3.BY_TIMESTAMPS) {
            mockStateManager.setState(Map.of(ListS3.CURRENT_TIMESTAMP, Long.toString(TEST_TIMESTAMP), ListS3.CURRENT_KEY_PREFIX + "0", "file"), Scope.CLUSTER);
        } else if (listingStrategy == ListS3.BY_ENTITIES) {
            String serviceId = "DistributedMapCacheClient";
            when(mockCache.getIdentifier()).thenReturn(serviceId);
            runner.addControllerService(serviceId, mockCache);
            runner.enableControllerService(mockCache);
            runner.setProperty(ListS3.TRACKING_STATE_CACHE, serviceId);
        }

        ListObjectsResponse response = ListObjectsResponse.builder()
                .isTruncated(false)
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(response);
    }

    @Test
    void testMigration() {
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed =
                Map.ofEntries(Map.entry("listing-strategy", ListS3.LISTING_STRATEGY.getName()),
                        Map.entry("delimiter", ListS3.DELIMITER.getName()),
                        Map.entry("prefix", ListS3.PREFIX.getName()),
                        Map.entry("use-versions", ListS3.USE_VERSIONS.getName()),
                        Map.entry("list-type", ListS3.LIST_TYPE.getName()),
                        Map.entry("min-age", ListS3.MIN_AGE.getName()),
                        Map.entry("max-age", ListS3.MAX_AGE.getName()),
                        Map.entry("write-s3-object-tags", ListS3.WRITE_OBJECT_TAGS.getName()),
                        Map.entry("requester-pays", ListS3.REQUESTER_PAYS.getName()),
                        Map.entry("write-s3-user-metadata", ListS3.WRITE_USER_METADATA.getName()),
                        Map.entry("record-writer", ListS3.RECORD_WRITER.getName()),
                        Map.entry("canned-acl", AbstractS3Processor.CANNED_ACL.getName()),
                        Map.entry("encryption-service", AbstractS3Processor.ENCRYPTION_SERVICE.getName()),
                        Map.entry("use-chunked-encoding", AbstractS3Processor.USE_CHUNKED_ENCODING.getName()),
                        Map.entry("use-path-style-access", AbstractS3Processor.USE_PATH_STYLE_ACCESS.getName()),
                        Map.entry("aws-region", REGION.getName()),
                        Map.entry(AbstractAwsProcessor.OBSOLETE_AWS_CREDENTIALS_PROVIDER_SERVICE_PROPERTY_NAME, AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE.getName()),
                        Map.entry(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, ListS3.TRACKING_STATE_CACHE.getName()),
                        Map.entry(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, ListS3.TRACKING_TIME_WINDOW.getName()),
                        Map.entry(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, ListS3.INITIAL_LISTING_TARGET.getName()),
                        Map.entry(ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE)
                );

        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());

        Set<String> expectedRemoved = Set.of(
                "Signer Override",
                "custom-signer-class-name",
                "Custom Signer Class Name",
                "custom-signer-module-location",
                "Custom Signer Module Location",
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_ACCESS_KEY.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_SECRET_KEY.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_CREDENTIALS_FILE.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_HOST.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_PORT.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_USERNAME.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_PASSWORD.getValue()
        );

        assertEquals(expectedRemoved, propertyMigrationResult.getPropertiesRemoved());
    }
}
