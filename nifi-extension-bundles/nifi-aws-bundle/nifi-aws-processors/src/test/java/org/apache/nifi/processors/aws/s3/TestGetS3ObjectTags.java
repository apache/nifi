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
import org.apache.nifi.processors.aws.s3.api.TagsTarget;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestGetS3ObjectTags {
    private TestRunner runner;

    private S3Client mockS3Client;

    private GetObjectTaggingResponse mockTags;

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
        GetS3ObjectTags mockGetS3ObjectTags = new GetS3ObjectTags() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockGetS3ObjectTags);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");

        mockTags = mock(GetObjectTaggingResponse.class);
        List<Tag> tags = List.of(Tag.builder().key("foo").value("bar").build(), Tag.builder().key("zip").value("zap").build());
        when(mockTags.tagSet()).thenReturn(tags);
    }

    private void run() {
        runner.setProperty(GetS3ObjectTags.BUCKET_WITH_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(GetS3ObjectTags.KEY, "${filename}");

        runner.setProperty(GetS3ObjectTags.VERSION_ID, "${versionId}");
        runner.enqueue("", Map.of("s3.bucket", "test-data", "filename", "test.txt", "versionId", "a-version"));

        runner.run();
    }

    private List<Tag> setupObjectTags() {
        final List<Tag> rawTags = List.of(Tag.builder().key("raw1").value("x").build(), Tag.builder().key("user2").value("y").build(), Tag.builder().key("mightHaveTo").value("z").build());

        when(mockTags.tagSet()).thenReturn(rawTags);

        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(mockTags);

        return rawTags;
    }

    @DisplayName("Validate fetch tags to attribute routes to found when the file exists")
    @Test
    void testFetchTagsToAttributeExists() {
        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, TagsTarget.ATTRIBUTES);
        runner.setProperty(GetS3ObjectTags.ATTRIBUTE_INCLUDE_PATTERN, "");

        final List<Tag> tags = setupObjectTags();

        run();
        runner.assertTransferCount(GetS3ObjectTags.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetS3ObjectTags.REL_FOUND).getFirst();
        tags.forEach(tag -> {
            final String key = String.format("s3.tag.%s", tag.key());
            final String val = flowFile.getAttribute(key);
            assertEquals(tag.value(), val);
        });
    }

    @DisplayName("Validate attribute exclusion")
    @Test
    void testFetchTagsToAttributeExclusion() {
        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, TagsTarget.ATTRIBUTES);
        runner.setProperty(GetS3ObjectTags.ATTRIBUTE_INCLUDE_PATTERN, "(raw|user)");

        final List<Tag> tags = setupObjectTags();
        final List<Tag> mustHave = tags.stream().filter(tag -> !"mightHaveTo".equals(tag.key())).toList();

        run();
        runner.assertTransferCount(GetS3ObjectTags.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetS3ObjectTags.REL_FOUND).getFirst();
        mustHave.forEach(tag -> {
            final String key = String.format("s3.tag.%s", tag.key());
            final String val = flowFile.getAttribute(key);
            assertEquals(tag.value(), val);
        });

        assertNull(flowFile.getAttribute("s3.tag.mightHaveTo"));
    }

    @DisplayName("Validate fetch to attribute mode routes to failure on S3 error")
    @Test
    void testFetchTagsToAttributeS3Error() {
        final AwsServiceException exception = S3Exception.builder().message("test").statusCode(501).build();

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, TagsTarget.ATTRIBUTES);
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_FAILURE, 1);
    }

    @DisplayName("Validate fetch tags to attribute routes to not-found when when the file doesn't exist")
    @Test
    void testFetchTagsToAttributeNotExist() {
        final AwsServiceException exception = S3Exception.builder().message("test").statusCode(404).build();

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, TagsTarget.ATTRIBUTES);
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_NOT_FOUND, 1);
    }

    @DisplayName("Validate fetch tags to FlowFile body routes to found when the file exists")
    @Test
    void testFetchTagsToBodyExists() {
        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, TagsTarget.FLOWFILE_BODY);
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(mockTags);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_FOUND, 1);
    }

    @DisplayName("Validate fetch tags to FlowFile body routes to not-found when when the file doesn't exist")
    @Test
    void testFetchTagsToBodyNotExist() {
        final AwsServiceException exception = S3Exception.builder().message("test").statusCode(404).build();

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, TagsTarget.FLOWFILE_BODY);
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_NOT_FOUND, 1);
    }

    @DisplayName("Validate fetch to FlowFile body mode routes to failure on S3 error")
    @Test
    void testFetchTagsToBodyS3Error() {
        final AwsServiceException exception = S3Exception.builder().message("test").statusCode(501).build();

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, TagsTarget.FLOWFILE_BODY);
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_FAILURE, 1);
    }
}
