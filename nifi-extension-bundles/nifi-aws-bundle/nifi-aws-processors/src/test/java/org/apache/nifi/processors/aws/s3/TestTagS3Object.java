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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.util.RegionUtilV1;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestTagS3Object {

    private TestRunner runner = null;
    private AmazonS3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        TagS3Object mockTagS3Object = new TagS3Object() {
            @Override
            protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final Region region, final ClientConfiguration config,
                                                  final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockTagS3Object);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testTagObjectSimple() {
        final String tagKey = "k";
        final String tagVal = "v";
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
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
        assertNull(request.getVersionId(), "test-version");
        assertTrue(request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)), "Expected tag not found in request");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag." + tagKey, tagVal);
    }

    @Test
    public void testTagObjectSimpleRegionFromFlowFileAttribute() {
        runner.setProperty(RegionUtilV1.S3_REGION, "attribute-defined-region");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, "k");
        runner.setProperty(TagS3Object.TAG_VALUE, "v");
        runner.setProperty(TagS3Object.APPEND_TAG, "false");
        runner.assertValid();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        attrs.put("s3.region", "us-east-1");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS);
    }

    @Test
    public void testTagObjectVersion() {
        final String tagKey = "k";
        final String tagVal = "v";
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
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
        assertTrue(request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)), "Expected tag not found in request");
    }

    @Test
    public void testTagObjectAppendToExistingTags() {
        //set up existing tags on S3 object
        Tag currentTag = new Tag("ck", "cv");
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        attrs.put("s3.tag." + currentTag.getKey(), currentTag.getValue());
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).setObjectTagging(captureRequest.capture());
        SetObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("object-key", request.getKey());
        assertTrue(request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)), "New tag not found in request");
        assertTrue(request.getTagging().getTagSet().contains(currentTag), "Existing tag not found in request");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag." + tagKey, tagVal);
        ff0.assertAttributeEquals("s3.tag." + currentTag.getKey(), currentTag.getValue());
    }

    @Test
    public void testTagObjectAppendUpdatesExistingTagValue() {
        //set up existing tags on S3 object
        Tag currentTag1 = new Tag("ck", "cv");
        Tag currentTag2 = new Tag("nk", "ov");
        mockGetExistingTags(currentTag1, currentTag2);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
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
        assertTrue(request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)), "New tag not found in request");
        assertTrue(request.getTagging().getTagSet().contains(currentTag1), "Existing tag not found in request");
        assertFalse(request.getTagging().getTagSet().contains(currentTag2), "Existing tag should be excluded from request");
    }

    @Test
    public void testTagObjectReplacesExistingTags() {
        //set up existing tags on S3 object
        Tag currentTag = new Tag("ck", "cv");
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        runner.setProperty(TagS3Object.APPEND_TAG, "false");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        attrs.put("s3.tag." + currentTag.getKey(), currentTag.getValue());
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<SetObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(SetObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).setObjectTagging(captureRequest.capture());
        SetObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());
        assertEquals("object-key", request.getKey());
        assertTrue(request.getTagging().getTagSet().contains(new Tag(tagKey, tagVal)), "New tag not found in request");
        assertFalse(request.getTagging().getTagSet().contains(currentTag), "Existing tag should be excluded from request");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag." + tagKey, tagVal);
        ff0.assertAttributeNotExists("s3.tag." + currentTag.getKey());
    }

    @Test
    public void testTagObjectS3Exception() {
        //set up existing tags on S3 object
        Tag currentTag = new Tag("ck", "cv");
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(new AmazonS3Exception("TagFailure")).when(mockS3Client).setObjectTagging(Mockito.any());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testBucketEvaluatedAsBlank() {
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "${not.existant.attribute}");
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
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
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
        runner.setProperty(RegionUtilV1.S3_REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, "tagKey");
        runner.setProperty(TagS3Object.TAG_VALUE, "${not.existant.attribute}");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    @Test
    void testMigration() {
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed =
                Map.of("custom-signer-class-name", AbstractS3Processor.S3_CUSTOM_SIGNER_CLASS_NAME.getName(),
                        "custom-signer-module-location", AbstractS3Processor.S3_CUSTOM_SIGNER_MODULE_LOCATION.getName(),
                        "tag-key", TagS3Object.TAG_KEY.getName(),
                        "tag-value", TagS3Object.TAG_VALUE.getName(),
                        "append-tag", TagS3Object.APPEND_TAG.getName());

        expectedRenamed.forEach((key, value) -> assertEquals(value, propertyMigrationResult.getPropertiesRenamed().get(key)));
    }

    private void mockGetExistingTags(Tag... currentTag) {
        List<Tag> currentTags = new ArrayList<>(Arrays.asList(currentTag));
        Mockito.when(mockS3Client.getObjectTagging(Mockito.any())).thenReturn(new GetObjectTaggingResult(currentTags));
    }

}
