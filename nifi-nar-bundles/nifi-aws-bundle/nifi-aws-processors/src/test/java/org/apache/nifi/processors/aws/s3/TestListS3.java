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

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
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
    public void testGetPropertyDescriptors() throws Exception {
        ListS3 processor = new ListS3();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 15, pd.size());
        assertTrue(pd.contains(ListS3.ACCESS_KEY));
        assertTrue(pd.contains(ListS3.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(ListS3.BUCKET));
        assertTrue(pd.contains(ListS3.CREDENTIALS_FILE));
        assertTrue(pd.contains(ListS3.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(ListS3.REGION));
        assertTrue(pd.contains(ListS3.SECRET_KEY));
        assertTrue(pd.contains(ListS3.SIGNER_OVERRIDE));
        assertTrue(pd.contains(ListS3.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(ListS3.TIMEOUT));
        assertTrue(pd.contains(ListS3.PROXY_HOST));
        assertTrue(pd.contains(ListS3.PROXY_HOST_PORT));
        assertTrue(pd.contains(ListS3.DELIMITER));
        assertTrue(pd.contains(ListS3.PREFIX));
        assertTrue(pd.contains(ListS3.USE_VERSIONS));
    }
}
