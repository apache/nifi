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
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestListS3 {

    private TestRunner runner = null;
    private ListS3 mockListS3 = null;
    private AmazonS3Client actualS3Client = null;
    private AmazonS3Client mockS3Client = null;

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        mockListS3 = new ListS3() {
            protected AmazonS3Client getClient() {
                actualS3Client = client;
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockListS3);
    }


    @Test
    public void testList() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");

        Date lastModified = new Date();
        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("a");
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        S3ObjectSummary objectSummary2 = new S3ObjectSummary();
        objectSummary2.setBucketName("test-bucket");
        objectSummary2.setKey("b/c");
        objectSummary2.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary2);
        S3ObjectSummary objectSummary3 = new S3ObjectSummary();
        objectSummary3.setBucketName("test-bucket");
        objectSummary3.setKey("d/e");
        objectSummary3.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary3);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertFalse(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListWithRecords() throws InitializationException {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");

        final MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.setProperty(ListS3.RECORD_WRITER, "record-writer");

        Date lastModified = new Date();
        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("a");
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        S3ObjectSummary objectSummary2 = new S3ObjectSummary();
        objectSummary2.setBucketName("test-bucket");
        objectSummary2.setKey("b/c");
        objectSummary2.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary2);
        S3ObjectSummary objectSummary3 = new S3ObjectSummary();
        objectSummary3.setBucketName("test-bucket");
        objectSummary3.setKey("d/e");
        objectSummary3.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary3);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertFalse(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);

        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        final String lastModifiedString = dateFormat.format(lastModified);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "3");
        flowFile.assertContentEquals("a,test-bucket,,," + lastModifiedString + ",0,,true,,,\n"
            + "b/c,test-bucket,,," + lastModifiedString + ",0,,true,,,\n"
            + "d/e,test-bucket,,," + lastModifiedString + ",0,,true,,,\n");
    }

    @Test
    public void testListWithRequesterPays() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.REQUESTER_PAYS, "true");

        Date lastModified = new Date();
        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("a");
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        S3ObjectSummary objectSummary2 = new S3ObjectSummary();
        objectSummary2.setBucketName("test-bucket");
        objectSummary2.setKey("b/c");
        objectSummary2.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary2);
        S3ObjectSummary objectSummary3 = new S3ObjectSummary();
        objectSummary3.setBucketName("test-bucket");
        objectSummary3.setKey("d/e");
        objectSummary3.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary3);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertTrue(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListWithRequesterPays_invalid() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.USE_VERSIONS, "true"); // requester pays cannot be used with versions
        runner.setProperty(ListS3.REQUESTER_PAYS, "true");

        runner.assertNotValid();
    }

    @Test
    public void testListVersion2() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.LIST_TYPE, "2");

        Date lastModified = new Date();
        ListObjectsV2Result objectListing = new ListObjectsV2Result();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("a");
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        S3ObjectSummary objectSummary2 = new S3ObjectSummary();
        objectSummary2.setBucketName("test-bucket");
        objectSummary2.setKey("b/c");
        objectSummary2.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary2);
        S3ObjectSummary objectSummary3 = new S3ObjectSummary();
        objectSummary3.setBucketName("test-bucket");
        objectSummary3.setKey("d/e");
        objectSummary3.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary3);
        Mockito.when(mockS3Client.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsV2Request> captureRequest = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjectsV2(captureRequest.capture());
        ListObjectsV2Request request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertFalse(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListVersion2WithRequesterPays() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.REQUESTER_PAYS, "true");
        runner.setProperty(ListS3.LIST_TYPE, "2");

        Date lastModified = new Date();
        ListObjectsV2Result objectListing = new ListObjectsV2Result();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("a");
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        S3ObjectSummary objectSummary2 = new S3ObjectSummary();
        objectSummary2.setBucketName("test-bucket");
        objectSummary2.setKey("b/c");
        objectSummary2.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary2);
        S3ObjectSummary objectSummary3 = new S3ObjectSummary();
        objectSummary3.setBucketName("test-bucket");
        objectSummary3.setKey("d/e");
        objectSummary3.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary3);
        Mockito.when(mockS3Client.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsV2Request> captureRequest = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjectsV2(captureRequest.capture());
        ListObjectsV2Request request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertTrue(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "a");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testListVersions() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.USE_VERSIONS, "true");

        Date lastModified = new Date();
        VersionListing versionListing = new VersionListing();
        S3VersionSummary versionSummary1 = new S3VersionSummary();
        versionSummary1.setBucketName("test-bucket");
        versionSummary1.setKey("test-key");
        versionSummary1.setVersionId("1");
        versionSummary1.setLastModified(lastModified);
        versionListing.getVersionSummaries().add(versionSummary1);
        S3VersionSummary versionSummary2 = new S3VersionSummary();
        versionSummary2.setBucketName("test-bucket");
        versionSummary2.setKey("test-key");
        versionSummary2.setVersionId("2");
        versionSummary2.setLastModified(lastModified);
        versionListing.getVersionSummaries().add(versionSummary2);
        Mockito.when(mockS3Client.listVersions(Mockito.any(ListVersionsRequest.class))).thenReturn(versionListing);

        runner.run();

        ArgumentCaptor<ListVersionsRequest> captureRequest = ArgumentCaptor.forClass(ListVersionsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listVersions(captureRequest.capture());
        ListVersionsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        Mockito.verify(mockS3Client, Mockito.never()).listObjects(Mockito.any(ListObjectsRequest.class));

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "test-key");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        ff0.assertAttributeEquals("s3.lastModified", String.valueOf(lastModified.getTime()));
        ff0.assertAttributeEquals("s3.version", "1");
        MockFlowFile ff1 = flowFiles.get(1);
        ff1.assertAttributeEquals("filename", "test-key");
        ff1.assertAttributeEquals("s3.bucket", "test-bucket");
        ff1.assertAttributeEquals("s3.lastModified", String.valueOf(lastModified.getTime()));
        ff1.assertAttributeEquals("s3.version", "2");
    }

    @Test
    public void testListObjectsNothingNew() throws IOException {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");

        Calendar calendar = Calendar.getInstance();
        calendar.set(2017, 5, 2);
        Date objectLastModified = calendar.getTime();
        long stateCurrentTimestamp = objectLastModified.getTime();

        Map<String, String> state = new HashMap<>();
        state.put(ListS3.CURRENT_TIMESTAMP, String.valueOf(stateCurrentTimestamp));
        state.put(ListS3.CURRENT_KEY_PREFIX+"0", "test-key");
        MockStateManager mockStateManager = runner.getStateManager();
        mockStateManager.setState(state, Scope.CLUSTER);

        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("test-key");
        objectSummary1.setLastModified(objectLastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 0);
    }


    @Test
    public void testListIgnoreByMinAge() throws IOException {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.MIN_AGE, "30 sec");

        Date lastModifiedNow = new Date();
        Date lastModifiedMinus1Hour = DateUtils.addHours(lastModifiedNow, -1);
        Date lastModifiedMinus3Hour = DateUtils.addHours(lastModifiedNow, -3);
        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("minus-3hour");
        objectSummary1.setLastModified(lastModifiedMinus3Hour);
        objectListing.getObjectSummaries().add(objectSummary1);
        S3ObjectSummary objectSummary2 = new S3ObjectSummary();
        objectSummary2.setBucketName("test-bucket");
        objectSummary2.setKey("minus-1hour");
        objectSummary2.setLastModified(lastModifiedMinus1Hour);
        objectListing.getObjectSummaries().add(objectSummary2);
        S3ObjectSummary objectSummary3 = new S3ObjectSummary();
        objectSummary3.setBucketName("test-bucket");
        objectSummary3.setKey("now");
        objectSummary3.setLastModified(lastModifiedNow);
        objectListing.getObjectSummaries().add(objectSummary3);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        Map<String,String> stateMap = new HashMap<>();
        String previousTimestamp = String.valueOf(lastModifiedMinus3Hour.getTime());
        stateMap.put(ListS3.CURRENT_TIMESTAMP, previousTimestamp);
        stateMap.put(ListS3.CURRENT_KEY_PREFIX + "0", "minus-3hour");
        runner.getStateManager().setState(stateMap, Scope.CLUSTER);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("filename", "minus-1hour");
        ff0.assertAttributeEquals("s3.bucket", "test-bucket");
        String lastModifiedTimestamp = String.valueOf(lastModifiedMinus1Hour.getTime());
        ff0.assertAttributeEquals("s3.lastModified", lastModifiedTimestamp);
        runner.getStateManager().assertStateEquals(ListS3.CURRENT_TIMESTAMP, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void testWriteObjectTags() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.WRITE_OBJECT_TAGS, "true");

        Date lastModified = new Date();
        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("a");
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);

        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<GetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(GetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObjectTagging(captureRequest.capture());
        GetObjectTaggingRequest request = captureRequest.getValue();

        assertEquals("test-bucket", request.getBucketName());
        assertEquals("a", request.getKey());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());
    }

    @Test
    public void testWriteUserMetadata() {
        runner.setProperty(ListS3.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET, "test-bucket");
        runner.setProperty(ListS3.WRITE_USER_METADATA, "true");

        Date lastModified = new Date();
        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName("test-bucket");
        objectSummary1.setKey("a");
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);

        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<GetObjectMetadataRequest> captureRequest = ArgumentCaptor.forClass(GetObjectMetadataRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObjectMetadata(captureRequest.capture());
        GetObjectMetadataRequest request = captureRequest.getValue();

        assertEquals("test-bucket", request.getBucketName());
        assertEquals("a", request.getKey());

        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());
    }
}
