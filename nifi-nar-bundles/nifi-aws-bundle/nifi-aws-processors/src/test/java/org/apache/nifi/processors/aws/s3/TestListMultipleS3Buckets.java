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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.ACCESS_KEY;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.BUCKET;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.COUNT;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.CREDENTIALS_FILE;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.CURRENT_KEY_PREFIX;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.CURRENT_TIMESTAMP;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.ENDPOINT_OVERRIDE;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.PROXY_HOST;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.PROXY_HOST_PORT;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.PROXY_PASSWORD;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.PROXY_USERNAME;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.REGION;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.REL_SUCCESS;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.REQUESTER_PAYS;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.SECRET_KEY;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.SIGNER_OVERRIDE;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.STATE_MGMT_KEY_DELIMITER;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.TIMEOUT;
import static org.apache.nifi.processors.aws.s3.ListMultipleS3Buckets.WRITE_USER_METADATA;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.nifi.processors.aws.s3.ListS3.DELIMITER;
import static org.apache.nifi.processors.aws.s3.ListS3.PREFIX;
import static org.apache.nifi.processors.aws.s3.ListS3.USE_VERSIONS;
import static org.apache.nifi.processors.aws.s3.ListS3.LIST_TYPE;
import static org.apache.nifi.processors.aws.s3.ListS3.MIN_AGE;
import static org.apache.nifi.processors.aws.s3.ListS3.WRITE_OBJECT_TAGS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestListMultipleS3Buckets {
    private AmazonS3Client mockS3Client = null;

    private static final Logger logger = LoggerFactory.getLogger(TestListMultipleS3Buckets.class);

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
    }

    static final String DEFAULT_REGION = "eu-west-1";
    static final String DEFAULT_BUCKET = "test-bucket-1";
    static final String DEFAULT_KEY_1 = "filePath1";
    static final String DEFAULT_KEY_2 = "filePath2";
    static final String DEFAULT_KEY_3 = "filePath3";

    private static final String ATTRIBUTE_FILENAME = "filename";
    private static final String ATTRIBUTE_S3_BUCKET = "s3.bucket";
    private static final String ATTRIBUTE_S3_LAST_MODIFIED = "s3.lastModified";
    private static final String ATTRIBUTE_S3_VERSION = "s3.version";

    @Test
    public void itShouldSetOutputFlowFileAttributesForOneBuckets() throws IOException {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);

        String key = DEFAULT_KEY_1;
        Date lastModified = new Date();
        ObjectListing objectListing =
                createFakeObjListingWith1Object(lastModified, key, bucketName);
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.enqueue(new byte[0], attrs);
        runner.run(1);

        runner.assertTransferCount(REL_SUCCESS, 1);
        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertThat(request.getBucketName(), equalTo(bucketName));
        verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        List<MockFlowFile> outputFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        assertThat(outputFlowFiles.size(), equalTo(1));
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());

        doFlowfileOneAttributeAsserts(outputFlowFiles, lastModifiedTimestamp);

        //Check that StateManager values were added correctly
        assertCorrectStateMgrValues1File(runner, bucketName, key, lastModifiedTimestamp);
    }

    @Test
    public void itShouldSetFlowFileAttributesForSameThreeBuckets() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);
        Date lastModified = new Date();
        ObjectListing objectListing = getFakeObjectListingWith3Objects(lastModified);
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.enqueue(new byte[0], attrs);
        runner.run();
        runner.assertQueueEmpty();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertThat(request.getBucketName(), equalTo(bucketName));
        assertFalse(request.isRequesterPays());
        verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertTransferCount(REL_SUCCESS, 3);
        List<MockFlowFile> outputFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());

        doFlowfileOneAttributeAsserts(outputFlowFiles, lastModifiedTimestamp);
        outputFlowFiles.get(1).assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_2);
        outputFlowFiles.get(2).assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_3);

        String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        runner.getStateManager().assertStateEquals(timestampKey, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void itShouldSetFlowFileAttributesForThreeDifferentBuckets() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);
        Date lastModified = new Date();
        ObjectListing objectListing1 =
                createFakeObjListingWith1Object(lastModified, DEFAULT_KEY_1, bucketName);

        String bucket2 = "test-bucket-2";
        String file2 = "s3-obj-2";
        ObjectListing objectListing2 =
                createFakeObjListingWith1Object(lastModified, bucket2, file2);
        final Map<String, String> ff2Attributes = new HashMap<>();
        ff2Attributes.put(ATTRIBUTE_S3_BUCKET, bucket2);

        String bucket3 = "test-bucket-3";
        String file3 = "s3-obj-3";
        ObjectListing objectListing3 =
                createFakeObjListingWith1Object(lastModified, bucket3, file3);
        final Map<String, String> ff3Attributes = new HashMap<>();
        ff3Attributes.put(ATTRIBUTE_S3_BUCKET, bucket3);

        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(objectListing1)
                .thenReturn(objectListing2)
                .thenReturn(objectListing3);

        final byte[] ffContent = new byte[0];
        runner.enqueue(ffContent, ff1Attributes);
        runner.enqueue(ffContent, ff2Attributes);
        runner.enqueue(ffContent, ff3Attributes);
        runner.run(3);
        runner.assertQueueEmpty();

        runner.assertTransferCount(REL_SUCCESS, 3);
        ArgumentCaptor<ListObjectsRequest> requestCaptor = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, Mockito.times(3)).listObjects(requestCaptor.capture());
        List<ListObjectsRequest> capturedRequests = requestCaptor.getAllValues();
        assertThat(capturedRequests.get(0).getBucketName(), equalTo(bucketName));
        assertThat(capturedRequests.get(1).getBucketName(), equalTo(bucket2));
        assertThat(capturedRequests.get(2).getBucketName(), equalTo(bucket3));
    }

    @Test
    public void itShouldManageStateForTwoInputFlowFilesFromDifferentBuckets() throws IOException {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucket1 = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucket1, s3ClientVersion);

        Date lastModified = new Date();
        String key1 = DEFAULT_KEY_1;
        final byte[] ffContent = new byte[0];
        ObjectListing objectListing1 =
                createFakeObjListingWith1Object(lastModified, key1, bucket1);

        String bucket2 = "test-bucket-2";
        String key2 = "s3-obj-2";
        String key3 = "s3-file-3";
        ObjectListing objectListing2 =
                createFakeObjListingWith2Objs1Bucket(lastModified, key2, key3, bucket2);
        final Map<String, String> ff2Attributes = new HashMap<>();
        ff2Attributes.put(ATTRIBUTE_S3_BUCKET, bucket2);

        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(objectListing1)
                .thenReturn(objectListing2);

        runner.enqueue(ffContent, ff1Attributes);
        runner.enqueue(ffContent, ff2Attributes);
        runner.run(2);
        runner.assertQueueEmpty();
        runner.assertTransferCount(REL_SUCCESS, 3);

        MockStateManager stateManager = runner.getStateManager();
        StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        Map<String, String> immutableState = stateMap.toMap();
        verifyTwoTimestampStateMapEntries(bucket1, bucket2,
                lastModifiedTimestamp, immutableState);
        assertThat(immutableState.size(), equalTo(7));
        verifyTwoBucketCountStateMapEntries(bucket1, bucket2, immutableState,
                1, 2);

        //Check for each of the s3Obj/file entries in the StateMap
        List<String> stateMapKeys = new ArrayList<>(immutableState.keySet());
        final String stateMapKey1 = bucket1 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(stateMapKeys, hasItem(stateMapKey1));
        final String stateMapKey2 = bucket2 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(stateMapKeys, hasItem(stateMapKey2));
        final String stateMapKey3 = bucket2 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 1;
        assertThat(stateMapKeys, hasItem(stateMapKey3));

        assertThat(immutableState.get(stateMapKey1), equalTo(key1));
        assertThat(immutableState.values(), hasItem(key2));
        assertThat(immutableState.get(stateMapKey2), startsWith("s3-"));
        assertThat(immutableState.values(), hasItem(key3));
        assertThat(immutableState.get(stateMapKey3), startsWith("s3-"));
    }

    @Test
    public void itShouldUpdateStateAfterTwoNewInputFlowFiles() throws IOException {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucket1 = DEFAULT_BUCKET;
        String bucket2 = "test-bucket-2";
        String newKey1 = "s3-obj-1";
        String oldObjectKey = "s3-obj-2";
        String newKey2 = "s3-file-3";
        String s3ClientVersion = "1";

        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucket1, s3ClientVersion);
        Date lastModified = new Date();
        ObjectListing objectListingBucket1 =
                createFakeObjListingWith1Object(lastModified, newKey1, bucket1);
        ObjectListing objectListingBucket2 =
                createFakeObjListingWith1Object(lastModified, newKey2, bucket2);
        final Map<String, String> ff2Attributes = new HashMap<>();
        ff2Attributes.put(ATTRIBUTE_S3_BUCKET, bucket2);
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(objectListingBucket1)
                .thenReturn(objectListingBucket2);

        Date lastModTimeMinus6Hour = DateUtils.addHours(lastModified, -6);
        setupStateMgrWithOneFileEntry(runner, bucket1, lastModTimeMinus6Hour,
                oldObjectKey);

        final byte[] ffContent = new byte[0];
        runner.enqueue(ffContent, ff1Attributes);
        runner.enqueue(ffContent, ff2Attributes);
        runner.run(2);
        runner.assertQueueEmpty();
        runner.assertTransferCount(REL_SUCCESS, 2);

        MockStateManager stateManager = runner.getStateManager();
        StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        Map<String, String> updatedState = stateMap.toMap();
        String lastModifiedStr = String.valueOf(lastModified.getTime());

        verifyTwoTimestampStateMapEntries(bucket1, bucket2, lastModifiedStr,
                updatedState);
        assertThat(updatedState.size(), equalTo(6));
        verifyTwoBucketCountStateMapEntries(bucket1, bucket2, updatedState,
                1, 1);

        List<String> stateMapKeys = new ArrayList<>(updatedState.keySet());
        final String stateMapKey1 = bucket1 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(stateMapKeys, hasItem(stateMapKey1));
        final String stateMapKey2 = bucket2 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(stateMapKeys, hasItem(stateMapKey2));

        assertThat(updatedState.get(stateMapKey1), equalTo(newKey1));
        assertThat(updatedState.get(stateMapKey2), equalTo(newKey2));
    }

    @Test
    public void itShouldRetrieveUpdatedStateForOnlyThisBucket() throws IOException {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucket1 = "test-bucket-1";
        String bucket2 = "test-bucket-2";
        String obj1Name = DEFAULT_KEY_1;
        String obj2Name = "test-obj-2";
        String obj3Name = "test-obj-3";
        String newObjectName = "new=test-object";
        String s3ClientVersion = "1";

        //I. Setup StateMgr with 3 Pre-existing Objects for 2 buckets (2 Obj -> bucket1, 1 Obj -> bucket2)
        Date objectLastModified = getOlderLastModDate();
        long statePriorTimestamp = objectLastModified.getTime();
        MockStateManager mockStateManager = runner.getStateManager();

        //A. Add Pre-existing StateMap entries for bucket1
        Map<String, String> state =
                setupPreExistingStateOneObjEntry(statePriorTimestamp, bucket1, obj1Name);
        String oldStateMapKey2 = bucket1 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + "1";
        state.put(oldStateMapKey2, obj2Name);
        String bucket1CountKey = bucket1 + STATE_MGMT_KEY_DELIMITER + COUNT;
        state.put(bucket1CountKey, String.valueOf(2));

        //B. Add Pre-existing StateMap entries for bucket2
        addPreExistingStateOneObjEntry(state, statePriorTimestamp, bucket2, obj3Name);
        String bucket2CountKey = bucket2 + STATE_MGMT_KEY_DELIMITER + COUNT;
        state.put(bucket2CountKey, String.valueOf(1));
        mockStateManager.setState(state, Scope.CLUSTER);

        Date newUpdatedDate = new Date();
        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucket1, s3ClientVersion);
        ObjectListing objectListingNewBucket =
                createFakeObjListingWith1Object(newUpdatedDate, newObjectName, bucket1);
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(objectListingNewBucket);

        final byte[] ffContent = new byte[0];
        runner.enqueue(ffContent, ff1Attributes);
        runner.run(1);
        runner.assertQueueEmpty();
        runner.assertTransferCount(REL_SUCCESS, 1);

        Date newStateTimestamp = new Date();
        MockStateManager stateManager = runner.getStateManager();
        StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        Map<String, String> updatedState = stateMap.toMap();
        assertThat(updatedState.size(), equalTo(6));

        //II. Verify StateMap Updates are correct
        //A. Verify bucket1 StateMap values are Now correct
        String bucket1TimestampKey = bucket1 + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        assertThat(Long.valueOf(updatedState.get(bucket1TimestampKey)), lessThan(newStateTimestamp.getTime()));    //Timestamp value HAS been Updated
        List<String> stateMapKeys = new ArrayList<>(updatedState.keySet());
        final String stateMapKey1 = bucket1 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(updatedState.get(stateMapKey1), equalTo(newObjectName));
        final String invalidStateMapKey = bucket1 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 1;
        assertThat(stateMapKeys, not(hasItem(invalidStateMapKey)));

        //B. Verify bucket2 StateMap values Have NOT changed
        String bucket2TimestampKey = bucket2 + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        assertThat(Long.valueOf(updatedState.get(bucket2TimestampKey)), equalTo(statePriorTimestamp));       //Timestamp value has NOT been Updated
        final String stateMapKey2 = bucket2 + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(updatedState.get(stateMapKey2), equalTo(obj3Name));
    }

    //TODO: new test -> itShouldUpdateTheStateManagerAdding2NewObjEntriesFor1Bucket

    @Test
    public void itShouldThrowRuntimeErrorIfItCannotGetStateMap() throws Exception {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucketName, "1");
        ObjectListing objectListing1 =
                createFakeObjListingWith1Object(new Date(), DEFAULT_KEY_1, bucketName);

        MockStateManager mockStateManager = runner.getStateManager();
        mockStateManager.setFailOnStateGet(Scope.CLUSTER, true);

        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(objectListing1);
        final byte[] ffContent = new byte[0];
        try {
            runner.enqueue(ffContent, ff1Attributes);
            runner.run(1);
            runner.assertTransferCount(REL_SUCCESS, 0);
        } catch (Throwable t) {
            assertTrue(t instanceof  AssertionError);
            assertTrue(t.getCause() instanceof RuntimeException);
            assertTrue(t.getCause().getCause() instanceof IOException);
        }
    }

    @Test
    public void itShouldThrowRuntimeErrorIfItCannotSetStateMap() throws Exception {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucketName, "1");
        ObjectListing objectListing1 =
                createFakeObjListingWith1Object(new Date(), DEFAULT_KEY_1, bucketName);
        runner.getStateManager().setFailOnStateSet(Scope.CLUSTER, true);
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(objectListing1);
        final byte[] ffContent = new byte[0];

        try {
            runner.enqueue(ffContent, ff1Attributes);
            runner.run(1);
            runner.assertTransferCount(REL_SUCCESS, 0);
        } catch (Throwable t) {
            assertTrue(t instanceof  AssertionError);
            assertTrue(t.getCause() instanceof RuntimeException);
            assertTrue(t.getCause().getCause() instanceof IOException);
        }
    }

    @Test
    public void itShouldThrowRuntimeExceptionForInvalidBucketCountEntry() throws IOException {
        String bucketName = "test-bucket-1";
        String obj1Name = DEFAULT_KEY_1;
        String obj2Name = "test-obj-2";

        TestRunner runner = getTestRunnerWithNewMockWatcher();
        Date objectLastModified = getOlderLastModDate();
        long statePriorTimestamp = objectLastModified.getTime();
        Map<String, String> state =
                setupPreExistingStateOneObjEntry(statePriorTimestamp, bucketName, obj1Name);
        String bucket1CountKey = bucketName + STATE_MGMT_KEY_DELIMITER + COUNT;
        state.put(bucket1CountKey, "bad-count-value");
        runner.getStateManager().setState(state, Scope.CLUSTER);

        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucketName, "1");
        ObjectListing objectListing1 =
                createFakeObjListingWith1Object(new Date(), obj2Name, bucketName);
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(objectListing1);

        try {
            final byte[] ffContent = new byte[0];
            runner.enqueue(ffContent, ff1Attributes);
            runner.run(1);
        } catch (Throwable t) {
            assertTrue(t.getCause() instanceof RuntimeException);
            assertThat(t.getCause().getMessage(), containsString("BucketCount value is invalid"));
        }
        runner.assertTransferCount(REL_SUCCESS, 0);
    }

    @Test
    public void itShouldSendThreeObjectsWithRequesterPays() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);

        runner.setProperty(REQUESTER_PAYS, "true");
        runner.enqueue(new byte[0], attrs);

        Date lastModified = new Date();
        ObjectListing objectListing = getFakeObjectListingWith3Objects(lastModified);
        when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals(request.getBucketName(), bucketName);
        assertTrue(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertTransferCount(REL_SUCCESS, 3);
        List<MockFlowFile> outputFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        doFlowfileOneAttributeAsserts(outputFlowFiles, lastModifiedTimestamp);

        outputFlowFiles.get(1).assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_2);
        outputFlowFiles.get(2).assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_3);

        String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        runner.getStateManager().assertStateEquals(timestampKey, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test
    public void itShouldBeInvalidForRequesterPaysTrueWithVersions() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        runner.setProperty(REGION, DEFAULT_REGION);
        runner.setProperty(BUCKET, "${s3.bucket}");
        runner.setProperty(USE_VERSIONS, "true"); // requester pays cannot be used with s3.versions
        runner.setProperty(REQUESTER_PAYS, "true");
        runner.assertNotValid();
    }

    @Test
    public void s3Version2ShouldSendThreeFlowfileObjectsOutputFor1InputBucket() throws IOException {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "2";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);

        runner.enqueue(new byte[0], attrs);

        Date lastModified = new Date();
        ListObjectsV2Result objectListing = getV2FakeObjListingWith3Objects(lastModified);
        when(mockS3Client.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsV2Request> captureRequest = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjectsV2(captureRequest.capture());
        ListObjectsV2Request request = captureRequest.getValue();
        assertEquals(request.getBucketName(), bucketName);
        assertFalse(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertTransferCount(REL_SUCCESS, 3);
        List<MockFlowFile> outputFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());

        doFlowfileOneAttributeAsserts(outputFlowFiles, lastModifiedTimestamp);

        String s3Object1 = DEFAULT_KEY_1;
        String s3Object2 = DEFAULT_KEY_2;
        String s3Object3 = DEFAULT_KEY_3;
        outputFlowFiles.get(0).assertAttributeEquals(ATTRIBUTE_FILENAME, s3Object1);
        outputFlowFiles.get(1).assertAttributeEquals(ATTRIBUTE_FILENAME, s3Object2);
        outputFlowFiles.get(2).assertAttributeEquals(ATTRIBUTE_FILENAME, s3Object3);

        final StateMap state = runner.getStateManager().getState(Scope.CLUSTER);
        Map<String, String> immutableState = state.toMap();
        assertThat(immutableState.size(), equalTo(5));

        final String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        runner.getStateManager().assertStateEquals(timestampKey, lastModifiedTimestamp, Scope.CLUSTER);
        final String bucket1CountKey = bucketName + STATE_MGMT_KEY_DELIMITER + COUNT;
        assertThat(Integer.valueOf(immutableState.get(bucket1CountKey)), equalTo(3));

        List<String> stateMapKeys = new ArrayList<>(immutableState.keySet());
        final String stateMapKey1 = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(stateMapKeys, hasItem(stateMapKey1));
        final String stateMapKey2 = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 1;
        assertThat(stateMapKeys, hasItem(stateMapKey2));
        final String stateMapKey3 = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 2;
        assertThat(stateMapKeys, hasItem(stateMapKey3));

        assertThat(immutableState.values(), hasItem(s3Object1));
        assertThat(immutableState.values(), hasItem(s3Object2));
        assertThat(immutableState.values(), hasItem(s3Object3));
    }

    @Test
    public void s3Version2ShouldSendThreeObjectsWithRequesterPays() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "2";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);

        runner.setProperty(REQUESTER_PAYS, "true");
        runner.enqueue(new byte[0], attrs);

        Date lastModified = new Date();
        ListObjectsV2Result objectListing = getV2FakeObjListingWith3Objects(lastModified);
        Mockito.when(mockS3Client.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsV2Request> captureRequest = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjectsV2(captureRequest.capture());
        ListObjectsV2Request request = captureRequest.getValue();
        assertEquals(request.getBucketName(), bucketName);
        assertTrue(request.isRequesterPays());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertTransferCount(REL_SUCCESS, 3);

        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        doFlowfileOneAttributeAsserts(flowFiles, lastModifiedTimestamp);

        flowFiles.get(1).assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_2);
        flowFiles.get(2).assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_3);

        String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        runner.getStateManager().assertStateEquals(timestampKey, lastModifiedTimestamp, Scope.CLUSTER);
    }

    @Test public void itShouldOutput2FlowfilesForV2ClientWithDifferentBucketNames() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucket1 = DEFAULT_BUCKET;
        String bucket2 = "test-bucket-2";
        String file1 = DEFAULT_KEY_1;
        String file2 = "se-obj-2";
        String s3ClientVersion = "2";

        final Map<String, String> ff1Attributes =
                setupPropertiesAndAttributes(runner, bucket1, s3ClientVersion);

        final byte[] ffContent = new byte[0];
        Date lastModified = new Date();

        ListObjectsV2Result objectListing1 = getV2FakeObjListingWith1Object(lastModified, file1, bucket1);
        ListObjectsV2Result objectListing2 = getV2FakeObjListingWith1Object(lastModified, file2, bucket2);
        final Map<String, String> ff2Attributes = new HashMap<>();
        ff2Attributes.put(ATTRIBUTE_S3_BUCKET, bucket2);

        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(objectListing1)
                .thenReturn(objectListing2);

        runner.enqueue(ffContent, ff1Attributes);
        runner.enqueue(ffContent, ff2Attributes);
        runner.run(2);
        runner.assertQueueEmpty();

        runner.assertTransferCount(REL_SUCCESS, 2);

        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile flowFile1 = flowFiles.get(0);
        flowFile1.assertAttributeEquals(ATTRIBUTE_FILENAME, file1);
        flowFile1.assertAttributeEquals(ATTRIBUTE_S3_BUCKET, bucket1);
        flowFile1.assertAttributeEquals(ATTRIBUTE_S3_LAST_MODIFIED, lastModifiedTimestamp);

        MockFlowFile flowFile2 = flowFiles.get(1);
        flowFile2.assertAttributeEquals(ATTRIBUTE_FILENAME, file2);
        flowFile2.assertAttributeEquals(ATTRIBUTE_S3_BUCKET, bucket2);
        flowFile2.assertAttributeEquals(ATTRIBUTE_S3_LAST_MODIFIED, lastModifiedTimestamp);
    }

    @Test
    public void itShouldOutput2FlowfilesUsingDifferentClientVersionsForOneBucket() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);

        runner.setProperty(USE_VERSIONS, "true");
        runner.enqueue(new byte[0], attrs);

        Date lastModified = new Date();
        VersionListing versionListing = new VersionListing();
        S3VersionSummary versionSummary1 = new S3VersionSummary();
        versionSummary1.setBucketName(DEFAULT_BUCKET);
        versionSummary1.setKey(DEFAULT_KEY_1);
        versionSummary1.setVersionId("1");
        versionSummary1.setLastModified(lastModified);
        versionListing.getVersionSummaries().add(versionSummary1);

        S3VersionSummary versionSummary2 = new S3VersionSummary();
        versionSummary2.setBucketName(DEFAULT_BUCKET);
        versionSummary2.setKey(DEFAULT_KEY_2);
        versionSummary2.setVersionId("2");
        versionSummary2.setLastModified(lastModified);
        versionListing.getVersionSummaries().add(versionSummary2);
        Mockito.when(mockS3Client.listVersions(Mockito.any(ListVersionsRequest.class))).thenReturn(versionListing);

        runner.run();

        ArgumentCaptor<ListVersionsRequest> captureRequest = ArgumentCaptor.forClass(ListVersionsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listVersions(captureRequest.capture());
        ListVersionsRequest request = captureRequest.getValue();
        assertEquals(request.getBucketName(), DEFAULT_BUCKET);
        Mockito.verify(mockS3Client, Mockito.never()).listObjects(Mockito.any(ListObjectsRequest.class));

        runner.assertTransferCount(REL_SUCCESS, 2);
        String lastModifiedTimestamp = String.valueOf(lastModified.getTime());
        List<MockFlowFile> outputFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        doFlowfileOneAttributeAsserts(outputFlowFiles, lastModifiedTimestamp);
        outputFlowFiles.get(0).assertAttributeEquals(ATTRIBUTE_S3_VERSION, "1");

        MockFlowFile ff1 = outputFlowFiles.get(1);
        ff1.assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_2);
        ff1.assertAttributeEquals(ATTRIBUTE_S3_BUCKET, DEFAULT_BUCKET);
        ff1.assertAttributeEquals(ATTRIBUTE_S3_LAST_MODIFIED, String.valueOf(lastModified.getTime()));
        ff1.assertAttributeEquals(ATTRIBUTE_S3_VERSION, "2");
    }

    @Test
    public void itShouldNotSendOutputFlowFileUponNoNewS3Objects() throws IOException {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);

        runner.enqueue(new byte[0], attrs);

        Date objectLastModified = getOlderLastModDate();
        long stateCurrentTimestamp = objectLastModified.getTime();
        String objName = DEFAULT_KEY_1;
        MockStateManager mockStateManager = runner.getStateManager();
        Map<String, String> state =
                setupPreExistingStateOneObjEntry(stateCurrentTimestamp, bucketName, objName);
        String bucketCountKey = bucketName + STATE_MGMT_KEY_DELIMITER + COUNT;
        state.put(bucketCountKey, String.valueOf(1));
        mockStateManager.setState(state, Scope.CLUSTER);

        ObjectListing objectListing =
                createFakeObjListingWith1Object(objectLastModified, objName, bucketName);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals(request.getBucketName(), DEFAULT_BUCKET);
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        runner.assertTransferCount(REL_SUCCESS, 0);
    }


    /*
     * In this Test we ignore 2 of the Three Objects in the returned S3-Obj List:
     *   1. the "now" object does not meet the Requirements of the Minimun Age of "30 seconds ago"
     *   2. the Oldest object entry in the list: the entry that is "now-3 hours" has already been found & is in the State Mgr already.
     */
    @Test
    public void itShouldIgnoreObjectsThatAreNotMinimumAge() throws IOException {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);
        runner.setProperty(MIN_AGE, "30 sec");
        runner.enqueue(new byte[0], attrs);

        Date lastModifiedNow = new Date();
        Date lastModifiedMinus1Hour = DateUtils.addHours(lastModifiedNow, -1);
        Date lastModifiedMinus3Hour = DateUtils.addHours(lastModifiedNow, -3);

        String foundObject = "minus-1hour";         //The 1 object that is NEW from the S3 Object List (NOT in the State Mgr) && > Min Age
        String oldObjectName = "minus-3hour";       //This one is Already in the State Mgr
        String filteredObject = "now";
        ObjectListing objectListing = new ObjectListing();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName(bucketName);
        objectSummary1.setKey(oldObjectName);
        objectSummary1.setLastModified(lastModifiedMinus3Hour);
        objectListing.getObjectSummaries().add(objectSummary1);
        S3ObjectSummary objectSummary2 = new S3ObjectSummary();
        objectSummary2.setBucketName(bucketName);
        objectSummary2.setKey(foundObject);
        objectSummary2.setLastModified(lastModifiedMinus1Hour);
        objectListing.getObjectSummaries().add(objectSummary2);
        S3ObjectSummary objectSummary3 = new S3ObjectSummary();
        objectSummary3.setBucketName(bucketName);
        objectSummary3.setKey(filteredObject);
        objectSummary3.setLastModified(lastModifiedNow);
        objectListing.getObjectSummaries().add(objectSummary3);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        //Set "Previous" State of the S3BucketsWatcher to already have one S3 Object/File entry for the "minus-3hour" file
        final String timestampKey = setupStateMgrWithOneFileEntry(runner,
                bucketName, lastModifiedMinus3Hour, oldObjectName);

        runner.run();

        ArgumentCaptor<ListObjectsRequest> captureRequest = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).listObjects(captureRequest.capture());
        ListObjectsRequest request = captureRequest.getValue();
        assertEquals(request.getBucketName(), bucketName);
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());

        String lastModifiedTimestamp = String.valueOf(lastModifiedMinus1Hour.getTime());

        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> outputFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff0 = outputFlowFiles.get(0);
        ff0.assertAttributeEquals(ATTRIBUTE_FILENAME, foundObject);
        ff0.assertAttributeEquals(ATTRIBUTE_S3_BUCKET, bucketName);
        ff0.assertAttributeEquals(ATTRIBUTE_S3_LAST_MODIFIED, lastModifiedTimestamp);

        runner.getStateManager().assertStateEquals(timestampKey, lastModifiedTimestamp, Scope.CLUSTER);

        //Confirm that StateManager values were added correctly
        assertCorrectStateMgrValues1File(runner, bucketName, foundObject, lastModifiedTimestamp);
    }

    @Test
    public void itShouldWriteObjectTags() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);
        runner.setProperty(WRITE_OBJECT_TAGS, "true");
        runner.enqueue(new byte[0], attrs);

        Date lastModified = new Date();
        ObjectListing objectListing =
                createFakeObjListingWith1Object(lastModified, DEFAULT_KEY_1, bucketName);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<GetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(GetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObjectTagging(captureRequest.capture());
        GetObjectTaggingRequest request = captureRequest.getValue();

        assertEquals(bucketName, request.getBucketName());
        assertEquals(DEFAULT_KEY_1, request.getKey());
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());
    }

    @Test
    public void testWriteUserMetadata() {
        TestRunner runner = getTestRunnerWithNewMockWatcher();
        String bucketName = DEFAULT_BUCKET;
        String s3ClientVersion = "1";
        final Map<String, String> attrs =
                setupPropertiesAndAttributes(runner, bucketName, s3ClientVersion);
        runner.setProperty(WRITE_USER_METADATA, "true");
        runner.enqueue(new byte[0], attrs);

        Date lastModified = new Date();
        ObjectListing objectListing =
                createFakeObjListingWith1Object(lastModified, DEFAULT_KEY_1, DEFAULT_BUCKET);
        Mockito.when(mockS3Client.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);

        runner.run();

        ArgumentCaptor<GetObjectMetadataRequest> captureRequest = ArgumentCaptor.forClass(GetObjectMetadataRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).getObjectMetadata(captureRequest.capture());
        GetObjectMetadataRequest request = captureRequest.getValue();

        assertEquals(request.getBucketName(), DEFAULT_BUCKET);
        assertEquals(request.getKey(), DEFAULT_KEY_1);
        Mockito.verify(mockS3Client, Mockito.never()).listVersions(Mockito.any());
    }

    @Test
    public void testGetPropertyDescriptors() {
        ListMultipleS3Buckets processor = new ListMultipleS3Buckets();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 23, pd.size());
        assertTrue(pd.contains(ACCESS_KEY));
        assertTrue(pd.contains(AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(BUCKET));
        assertTrue(pd.contains(CREDENTIALS_FILE));
        assertTrue(pd.contains(ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(REGION));
        assertTrue(pd.contains(WRITE_OBJECT_TAGS));
        assertTrue(pd.contains(WRITE_USER_METADATA));
        assertTrue(pd.contains(SECRET_KEY));
        assertTrue(pd.contains(SIGNER_OVERRIDE));
        assertTrue(pd.contains(SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(TIMEOUT));
        assertTrue(pd.contains(DELIMITER));
        assertTrue(pd.contains(PREFIX));
        assertTrue(pd.contains(USE_VERSIONS));
        assertTrue(pd.contains(LIST_TYPE));
        assertTrue(pd.contains(MIN_AGE));
        assertTrue(pd.contains(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE));
        assertTrue(pd.contains(PROXY_HOST));
        assertTrue(pd.contains(PROXY_HOST_PORT));
        assertTrue(pd.contains(PROXY_USERNAME));
        assertTrue(pd.contains(PROXY_PASSWORD));
        assertTrue(pd.contains(REQUESTER_PAYS));
    }

    /* A Test to be used solely for the purpose of determining approximately
     * what is the maximum number of buckets that can be successfully persisted in the StateMap.
     *  <b>Assumptions:</b>
     *     1. Each bucket consists of 3 file/object entries, named:
     *         - objectOne  (9 chars long)
     *         - thisIsVeryLongFileName123-000-798-KittyCatZZq (45 chars)
     *         - someOtherObject88970 (20 chars)
     *
     *     2. Buckets will be named following this naming convention:
     *          bucket-0, bucket-1, bucket-2, ...
     *
     *     3.Note that to measure the size of the serialized StateMap value, I have "borrowed" the
     *      implementation from the current 1.10.0-SNAPSHOT version of the nifi-framework-core
     *      (in the org.apache.nifi.controller.state.providers.zookeeper.ZooKeeperStateProvider class)
     *
     *     4. Max StateMap serialized Size = 1,000,000 bytes (1MB)
     *
     * NOTE: we are ignoring this "TEst" here. uncomment the @Test annotation to run it yourself.
     * @See https://github.com/apache/nifi/blob/master/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-framework-core/src/main/java/org/apache/nifi/controller/state/providers/zookeeper/ZooKeeperStateProvider.java
     *    for the implementation of serialize() that we are using here
     *
     * @See http://nifi.apache.org/docs/nifi-docs/html/developer-guide.html#state_manager:
     *    "As such, the entire State Map must be less than 1 MB in size, after being serialized."
     *
     *   Result: According to this test, with the above assumptions ->
     *      MAX Bucket Size = 4698 buckets
     */
    @Ignore
    //@Test
    public void findMaxBucketSize() throws Exception {
        String fileOne = "objectOne";
        String fileTwo = "thisIsVeryLongFileName123-000-798-KittyCatZZq";
        String fileThree = "someOtherObject88970";
        String firstBucketName = "bucket-" + 0;

        System.out.println("====== ** Find Max Bucket Size Test ** ======");
        String timeStampStr = "1571855408843";  // = Approx 2:30pm on 10/23/2019
        Map<String, String> stateMap = getStateMapWithThreeFileEntriesOneBucket(
                firstBucketName, timeStampStr, fileOne, fileTwo, fileThree);

        int oneMegabyte = 1000000;
        int bucketNum = 1;
        int currentMapSize = 0;
        String bucketName = "";
        while (bucketNum < 4698) {
            bucketName = "bucket-" + bucketNum;
            stateMap = addNewBucketContentToStateMap(stateMap, bucketName,
                    timeStampStr, fileOne, fileTwo, fileThree);
            byte[] serializedBytes = serialize(stateMap);
            currentMapSize = serializedBytes.length;
            if (currentMapSize > oneMegabyte) {
                break;
            }
            bucketNum++;
        }

        System.out.println("=========== Serialized StateMap ==========");
        System.out.println(" -> Size: " + currentMapSize);
        System.out.println("Maxed Out StateMap -> BucketNum: " + bucketNum + "| bucketName: " + bucketName);
    }

    private void doFlowfileOneAttributeAsserts(List<MockFlowFile> outputFlowFiles, String lastModifiedTimestamp) {
        MockFlowFile ff0 = outputFlowFiles.get(0);
        ff0.assertAttributeEquals(ATTRIBUTE_FILENAME, DEFAULT_KEY_1);
        ff0.assertAttributeEquals(ATTRIBUTE_S3_BUCKET, DEFAULT_BUCKET);
        ff0.assertAttributeEquals(ATTRIBUTE_S3_LAST_MODIFIED, lastModifiedTimestamp);
    }

    private TestRunner getTestRunnerWithNewMockWatcher() {
        ListMultipleS3Buckets mockBucketWatcher = new ListMultipleS3Buckets() {
            protected AmazonS3Client getClient() {
                return mockS3Client;
            }
        };
        return TestRunners.newTestRunner(mockBucketWatcher);
    }

    private ObjectListing getFakeObjectListingWith3Objects(Date lastModified) {
        ObjectListing objectListing =
                createFakeObjListingWith1Object(lastModified, DEFAULT_KEY_1, DEFAULT_BUCKET);
        objectListing = addnewObjSummaryToListing(lastModified, DEFAULT_KEY_2, DEFAULT_BUCKET, objectListing);
        return addnewObjSummaryToListing(lastModified, DEFAULT_KEY_3, DEFAULT_BUCKET, objectListing);
    }

    private ObjectListing createFakeObjListingWith1Object(Date objectLastModified,
                                                          String fileName, String bucketName) {
        ObjectListing objectListing = new ObjectListing();
        return addnewObjSummaryToListing(objectLastModified, fileName, bucketName, objectListing);
    }

    private ObjectListing createFakeObjListingWith2Objs1Bucket(Date objectLastModified,
                                                               String file1, String file2, String bucketName) {
        ObjectListing objectListing = new ObjectListing();
        objectListing = addnewObjSummaryToListing(objectLastModified, file1, bucketName, objectListing);
        return addnewObjSummaryToListing(objectLastModified, file2, bucketName, objectListing);
    }

    private ObjectListing addnewObjSummaryToListing(Date objectLastModified, String fileName,
                                                    String bucketName, ObjectListing objectListing) {
        S3ObjectSummary objectSummary = new S3ObjectSummary();
        objectSummary.setBucketName(bucketName);
        objectSummary.setKey(fileName);
        objectSummary.setLastModified(objectLastModified);
        objectListing.getObjectSummaries().add(objectSummary);
        return objectListing;
    }

    private ListObjectsV2Result getV2FakeObjListingWith3Objects(Date lastModified) {
        ListObjectsV2Result objectListing = new ListObjectsV2Result();

        objectListing = addnewObjSummaryToObjV2Listing(lastModified, DEFAULT_KEY_1, DEFAULT_BUCKET, objectListing);
        objectListing = addnewObjSummaryToObjV2Listing(lastModified, DEFAULT_KEY_2, DEFAULT_BUCKET, objectListing);
        return addnewObjSummaryToObjV2Listing(lastModified, DEFAULT_KEY_3, DEFAULT_BUCKET, objectListing);
    }

    private ListObjectsV2Result addnewObjSummaryToObjV2Listing(Date lastModified, String fileName,
                                                               String bucketName, ListObjectsV2Result objectListing) {

        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName(bucketName);
        objectSummary1.setKey(fileName);
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        return objectListing;
    }

    private ListObjectsV2Result getV2FakeObjListingWith1Object(Date lastModified, String fileName,
                                                               String bucketName) {
        ListObjectsV2Result objectListing = new ListObjectsV2Result();
        S3ObjectSummary objectSummary1 = new S3ObjectSummary();
        objectSummary1.setBucketName(bucketName);
        objectSummary1.setKey(fileName);
        objectSummary1.setLastModified(lastModified);
        objectListing.getObjectSummaries().add(objectSummary1);
        return objectListing;
    }

    private Map<String, String> setupPropertiesAndAttributes(TestRunner runner, String bucketName, String s3ClientVersion) {
        runner.setProperty(REGION, DEFAULT_REGION);
        runner.setProperty(BUCKET, "${s3.bucket}");
        runner.setProperty(LIST_TYPE, s3ClientVersion);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put(ATTRIBUTE_S3_BUCKET, bucketName);
        return attrs;
    }
    private Map<String,String> getStateMapWithThreeFileEntriesOneBucket(String bucketName,
                                                                        String timeStampStr, String objectOne,
                                                                        String objectTwo, String objectThree) throws IOException {

        Map<String,String> stateMap = new HashMap<>();
        return addNewBucketContentToStateMap(stateMap, bucketName, timeStampStr,
                objectOne, objectTwo, objectThree);
    }

    private Map<String,String> addNewBucketContentToStateMap(Map<String,String> stateMap, String bucketName,
                                                             String timeStampStr, String objectOne,
                                                             String objectTwo, String objectThree) throws IOException {

        final String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        final String objectKeyOne = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        final String objectKeyTwo = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 1;
        final String objectKeyThree = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 2;
        final String bucketCountKey = bucketName + STATE_MGMT_KEY_DELIMITER + COUNT;
        stateMap.put(timestampKey, timeStampStr);
        stateMap.put(objectKeyOne, objectOne);
        stateMap.put(objectKeyTwo, objectTwo);
        stateMap.put(objectKeyThree, objectThree);
        stateMap.put(bucketCountKey, String.valueOf(3));
        return stateMap;
    }

    private String setupStateMgrWithOneFileEntry(TestRunner runner, String bucketName,
                                                 Date objLastModTime, String objectName) throws IOException {

        Map<String,String> stateMap = new HashMap<>();
        String previousTimestamp = String.valueOf(objLastModTime.getTime());
        final String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        final String stateMapObjectKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        final String bucketCountKey = bucketName + STATE_MGMT_KEY_DELIMITER + COUNT;
        stateMap.put(timestampKey, previousTimestamp);
        stateMap.put(stateMapObjectKey, objectName);
        stateMap.put(bucketCountKey, String.valueOf(1));        //object "minus-3hour" is already in the StateMap
        runner.getStateManager().setState(stateMap, Scope.CLUSTER);
        return timestampKey;
    }

    private void assertCorrectStateMgrValues1File(TestRunner runner, String bucketName,
                                                  String objectName, String lastModifiedTimestamp) throws IOException {

        final StateMap state = runner.getStateManager().getState(Scope.CLUSTER);
        Map<String, String> immutableState = state.toMap();
        assertThat(immutableState.size(), equalTo(3));
        final String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        runner.getStateManager().assertStateEquals(timestampKey, lastModifiedTimestamp, Scope.CLUSTER);

        String bucket1CountKey = bucketName + STATE_MGMT_KEY_DELIMITER + COUNT;
        assertThat(Integer.valueOf(immutableState.get(bucket1CountKey)), equalTo(1));
        final String stateMapKey1 = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + 0;
        assertThat(immutableState.get(stateMapKey1), equalTo(objectName));
    }

    private void verifyTwoTimestampStateMapEntries(String bucket1, String bucket2,
                                                   String lastModifiedTimestamp, Map<String, String> immutableState) {
        String bucket1TimestampKey = bucket1 + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        String bucket2TimestampKey = bucket2 + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        assertThat(immutableState.get(bucket1TimestampKey), equalTo(lastModifiedTimestamp));
        assertThat(immutableState.get(bucket2TimestampKey), equalTo(lastModifiedTimestamp));
    }

    private void verifyTwoBucketCountStateMapEntries(String bucket1, String bucket2, Map<String, String> immutableState, int bucket1Count, int bucket2Count) {
        String bucket1CountKey = bucket1 + STATE_MGMT_KEY_DELIMITER + COUNT;
        String bucket2CountKey = bucket2 + STATE_MGMT_KEY_DELIMITER + COUNT;
        assertThat(Integer.valueOf(immutableState.get(bucket1CountKey)), equalTo(bucket1Count));
        assertThat(Integer.valueOf(immutableState.get(bucket2CountKey)), equalTo(bucket2Count));
    }

    private Map<String, String> setupPreExistingStateOneObjEntry(long stateTimestamp,
                                                                 String bucketName, String objName) {
        Map<String, String> state = new HashMap<>();
        return addPreExistingStateOneObjEntry(state, stateTimestamp, bucketName, objName);
    }

    private Map<String, String> addPreExistingStateOneObjEntry(Map<String, String> state, long stateTimestamp,
                                                               String bucketName, String objName) {
        String timestampKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
        String stateMapKey = bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX + "0";
        state.put(timestampKey, String.valueOf(stateTimestamp));
        state.put(stateMapKey, objName);
        return state;
    }

    private Date getOlderLastModDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2019, 7, 2);
        return calendar.getTime();
    }

    //NOTE: utility method to be used for Max Buckets computation
    private static final byte ENCODING_VERSION = 1;
    private byte[] serialize(final Map<String, String> stateValues) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(ENCODING_VERSION);
            dos.writeInt(stateValues.size());
            for (final Map.Entry<String, String> entry : stateValues.entrySet()) {
                final boolean hasKey = entry.getKey() != null;
                final boolean hasValue = entry.getValue() != null;
                dos.writeBoolean(hasKey);
                if (hasKey) {
                    dos.writeUTF(entry.getKey());
                }

                dos.writeBoolean(hasValue);
                if (hasValue) {
                    dos.writeUTF(entry.getValue());
                }
            }
            return baos.toByteArray();
        }
    }
}
