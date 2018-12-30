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
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestTagS3Object {

    private TestRunner runner = null;
    private TagS3Object mockTagS3Object = null;
    private AmazonS3Client actualS3Client = null;
    private AmazonS3Client mockS3Client = null;

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        mockTagS3Object = new TagS3Object() {
            protected AmazonS3Client getClient() {
                actualS3Client = client;
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockTagS3Object);
    }

    @Test
    public void testTagObjectSimple() throws IOException {
        final String tagKey = "k";
        final String tagVal = "v";
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        runner.setProperty(TagS3Object.APPEND_TAG, "false");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).setObjectTagging(captureRequest.capture());
        SetObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("object-key", request.getKey());
        assertNull("test-version", request.getVersionId());
        assertTrue("Expected tag not found in request", request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)));

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag."+tagKey, tagVal);
    }

    @Test
    public void testTagObjectVersion() throws IOException {
        final String tagKey = "k";
        final String tagVal = "v";
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.VERSION_ID, "test-version");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        runner.setProperty(TagS3Object.APPEND_TAG, "false");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).setObjectTagging(captureRequest.capture());
        SetObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("object-key", request.getKey());
        assertEquals("test-version", request.getVersionId());
        assertTrue("Expected tag not found in request", request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)));
    }

    @Test
    public void testTagObjectAppendToExistingTags() throws IOException {
        //set up existing tags on S3 object
        Tag currentTag = new Tag("ck", "cv");
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        attrs.put("s3.tag."+currentTag.getKey(), currentTag.getValue());
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).setObjectTagging(captureRequest.capture());
        SetObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("object-key", request.getKey());
        assertTrue("New tag not found in request", request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)));
        assertTrue("Existing tag not found in request", request.getTagging().getTagSet().contains(currentTag));

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag."+tagKey, tagVal);
        ff0.assertAttributeEquals("s3.tag."+currentTag.getKey(), currentTag.getValue());
    }

    @Test
    public void testTagObjectAppendUpdatesExistingTagValue() throws IOException {
        //set up existing tags on S3 object
        Tag currentTag1 = new Tag("ck", "cv");
        Tag currentTag2 = new Tag("nk", "ov");
        mockGetExistingTags(currentTag1, currentTag2);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).setObjectTagging(captureRequest.capture());
        SetObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("object-key", request.getKey());
        assertTrue("New tag not found in request", request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)));
        assertTrue("Existing tag not found in request", request.getTagging().getTagSet().contains(currentTag1));
        assertFalse("Existing tag should be excluded from request", request.getTagging().getTagSet().contains(currentTag2));
    }

    @Test
    public void testTagObjectReplacesExistingTags() throws IOException {
        //set up existing tags on S3 object
        Tag currentTag = new Tag("ck", "cv");
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        runner.setProperty(TagS3Object.APPEND_TAG, "false");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        attrs.put("s3.tag."+currentTag.getKey(), currentTag.getValue());
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).setObjectTagging(captureRequest.capture());
        SetObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("object-key", request.getKey());
        assertTrue("New tag not found in request", request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)));
        assertFalse("Existing tag should be excluded from request", request.getTagging().getTagSet().contains(currentTag));

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag."+tagKey, tagVal);
        ff0.assertAttributeNotExists("s3.tag."+currentTag.getKey());
    }

    @Test
    public void testTagObjectS3Exception() {
        //set up existing tags on S3 object
        Tag currentTag = new Tag("ck", "cv");
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(new AmazonS3Exception("TagFailure")).when(mockS3Client).setObjectTagging(Mockito.any());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
    }

    @Test
    public void testGetPropertyDescriptors() throws Exception {
        TagS3Object processor = new TagS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 20, pd.size());
        assertTrue(pd.contains(TagS3Object.ACCESS_KEY));
        assertTrue(pd.contains(TagS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(TagS3Object.BUCKET));
        assertTrue(pd.contains(TagS3Object.CREDENTIALS_FILE));
        assertTrue(pd.contains(TagS3Object.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(TagS3Object.KEY));
        assertTrue(pd.contains(TagS3Object.REGION));
        assertTrue(pd.contains(TagS3Object.SECRET_KEY));
        assertTrue(pd.contains(TagS3Object.SIGNER_OVERRIDE));
        assertTrue(pd.contains(TagS3Object.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(TagS3Object.TIMEOUT));
        assertTrue(pd.contains(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE));
        assertTrue(pd.contains(TagS3Object.PROXY_HOST));
        assertTrue(pd.contains(TagS3Object.PROXY_HOST_PORT));
        assertTrue(pd.contains(TagS3Object.PROXY_USERNAME));
        assertTrue(pd.contains(TagS3Object.PROXY_PASSWORD));
        assertTrue(pd.contains(TagS3Object.TAG_KEY));
        assertTrue(pd.contains(TagS3Object.TAG_VALUE));
        assertTrue(pd.contains(TagS3Object.APPEND_TAG));
        assertTrue(pd.contains(TagS3Object.VERSION_ID));
    }

    @Test
    public void testBucketEvaluatedAsBlank() {
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "${not.existant.attribute}");
        runner.setProperty(TagS3Object.TAG_KEY, "key");
        runner.setProperty(TagS3Object.TAG_VALUE, "val");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testTagKeyEvaluatedAsBlank() {
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, "${not.existant.attribute}");
        runner.setProperty(TagS3Object.TAG_VALUE, "val");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testTagValEvaluatedAsBlank() {
        runner.setProperty(TagS3Object.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, "tagKey");
        runner.setProperty(TagS3Object.TAG_VALUE, "${not.existant.attribute}");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    private void mockGetExistingTags(Tag... currentTag) {
        List<Tag> currentTags = new ArrayList<>(Arrays.asList(currentTag));
        Mockito.when(mockS3Client.getObjectTagging(Mockito.anyObject())).thenReturn(new GetObjectTaggingResult(currentTags));
    }
}
