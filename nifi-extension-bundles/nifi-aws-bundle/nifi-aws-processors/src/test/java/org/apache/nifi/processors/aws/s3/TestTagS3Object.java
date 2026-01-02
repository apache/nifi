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

import org.apache.nifi.processor.ProcessContext;
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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;

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
    private S3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = Mockito.mock(S3Client.class);
        Mockito.when(mockS3Client.utilities()).thenReturn(S3Utilities.builder().region(Region.US_WEST_2).build());
        TagS3Object mockTagS3Object = new TagS3Object() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
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
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        runner.setProperty(TagS3Object.APPEND_TAG, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<PutObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(PutObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObjectTagging(captureRequest.capture());
        PutObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("object-key", request.key());
        assertNull(request.versionId(), "test-version");
        assertTrue(request.tagging().tagSet().contains(Tag.builder().key(tagKey).value(tagVal).build()), "Expected tag not found in request");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag." + tagKey, tagVal);
    }

    @Test
    public void testTagObjectSimpleRegionFromFlowFileAttribute() {
        runner.setProperty(RegionUtil.REGION, "use-custom-region");
        runner.setProperty(RegionUtil.CUSTOM_REGION, "${s3.region}");
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
        runner.setProperty(RegionUtil.REGION, "us-west-2");
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
        ArgumentCaptor<PutObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(PutObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObjectTagging(captureRequest.capture());
        PutObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("object-key", request.key());
        assertEquals("test-version", request.versionId());
        assertTrue(request.tagging().tagSet().contains(Tag.builder().key(tagKey).value(tagVal).build()), "Expected tag not found in request");
    }

    @Test
    public void testTagObjectAppendToExistingTags() {
        //set up existing tags on S3 object
        Tag currentTag = Tag.builder().key("ck").value("cv").build();
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        attrs.put("s3.tag." + currentTag.key(), currentTag.value());
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<PutObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(PutObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObjectTagging(captureRequest.capture());
        PutObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("object-key", request.key());
        assertTrue(request.tagging().tagSet().contains(Tag.builder().key(tagKey).value(tagVal).build()), "New tag not found in request");
        assertTrue(request.tagging().tagSet().contains(currentTag), "Existing tag not found in request");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag." + tagKey, tagVal);
        ff0.assertAttributeEquals("s3.tag." + currentTag.key(), currentTag.value());
    }

    @Test
    public void testTagObjectAppendUpdatesExistingTagValue() {
        //set up existing tags on S3 object
        Tag currentTag1 = Tag.builder().key("ck").value("cv").build();
        Tag currentTag2 = Tag.builder().key("nk").value("ov").build();
        mockGetExistingTags(currentTag1, currentTag2);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<PutObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(PutObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObjectTagging(captureRequest.capture());
        PutObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("object-key", request.key());
        assertTrue(request.tagging().tagSet().contains(Tag.builder().key(tagKey).value(tagVal).build()), "New tag not found in request");
        assertTrue(request.tagging().tagSet().contains(currentTag1), "Existing tag not found in request");
        assertFalse(request.tagging().tagSet().contains(currentTag2), "Existing tag should be excluded from request");
    }

    @Test
    public void testTagObjectReplacesExistingTags() {
        //set up existing tags on S3 object
        Tag currentTag = Tag.builder().key("ck").value("cv").build();
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        runner.setProperty(TagS3Object.APPEND_TAG, "false");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "object-key");
        attrs.put("s3.tag." + currentTag.key(), currentTag.value());
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        ArgumentCaptor<PutObjectTaggingRequest> captureRequest = ArgumentCaptor.forClass(PutObjectTaggingRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObjectTagging(captureRequest.capture());
        PutObjectTaggingRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.bucket());
        assertEquals("object-key", request.key());
        assertTrue(request.tagging().tagSet().contains(Tag.builder().key(tagKey).value(tagVal).build()), "New tag not found in request");
        assertFalse(request.tagging().tagSet().contains(currentTag), "Existing tag should be excluded from request");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("s3.tag." + tagKey, tagVal);
        ff0.assertAttributeNotExists("s3.tag." + currentTag.key());
    }

    @Test
    public void testTagObjectS3Exception() {
        //set up existing tags on S3 object
        Tag currentTag = Tag.builder().key("ck").value("cv").build();
        mockGetExistingTags(currentTag);

        final String tagKey = "nk";
        final String tagVal = "nv";
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(TagS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "test-bucket");
        runner.setProperty(TagS3Object.TAG_KEY, tagKey);
        runner.setProperty(TagS3Object.TAG_VALUE, tagVal);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-key");
        runner.enqueue(new byte[0], attrs);
        Mockito.doThrow(S3Exception.builder().message("TagFailure").build()).when(mockS3Client).putObjectTagging(Mockito.any(PutObjectTaggingRequest.class));

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testBucketEvaluatedAsBlank() {
        runner.setProperty(RegionUtil.REGION, "us-west-2");
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
        runner.setProperty(RegionUtil.REGION, "us-west-2");
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
        runner.setProperty(RegionUtil.REGION, "us-west-2");
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
                Map.of("tag-key", TagS3Object.TAG_KEY.getName(),
                        "tag-value", TagS3Object.TAG_VALUE.getName(),
                        "append-tag", TagS3Object.APPEND_TAG.getName());

        expectedRenamed.forEach((key, value) -> assertEquals(value, propertyMigrationResult.getPropertiesRenamed().get(key)));
    }

    private void mockGetExistingTags(Tag... currentTag) {
        List<Tag> currentTags = new ArrayList<>(Arrays.asList(currentTag));
        Mockito.when(mockS3Client.getObjectTagging(Mockito.any(GetObjectTaggingRequest.class))).thenReturn(GetObjectTaggingResponse.builder().tagSet(currentTags).build());
    }

}
