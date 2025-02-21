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
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.Tag;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestGetS3ObjectTags {
    private TestRunner runner;

    private AmazonS3Client mockS3Client;

    private GetObjectTaggingResult mockTags;

    @BeforeEach
    void setUp() {
        mockS3Client = mock(AmazonS3Client.class);
        GetS3ObjectTags mockGetS3ObjectTags = new GetS3ObjectTags() {
            @Override
            protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final Region region, final ClientConfiguration config,
                                                  final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockGetS3ObjectTags);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");

        mockTags = mock(GetObjectTaggingResult.class);
        List<Tag> tags = List.of(new Tag("foo", "bar"), new Tag("zip", "zap"));
        when(mockTags.getTagSet()).thenReturn(tags);
    }

    private void run() {
        runner.setProperty(GetS3ObjectTags.BUCKET_WITH_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(GetS3ObjectTags.KEY, "${filename}");

        runner.setProperty(GetS3ObjectTags.VERSION_ID, "${versionId}");
        runner.enqueue("", Map.of("s3.bucket", "test-data", "filename", "test.txt", "versionId", "a-version"));

        runner.run();
    }

    private List<Tag> setupObjectTags() {
        final List<Tag> rawTags = List.of(new Tag("raw1", "x"), new Tag("user2", "y"), new Tag("mightHaveTo", "z"));

        when(mockTags.getTagSet()).thenReturn(rawTags);

        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(mockTags);

        return rawTags;
    }

    @DisplayName("Validate fetch tags to attribute routes to found when the file exists")
    @Test
    void testFetchTagsToAttributeExists() {
        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, GetS3ObjectTags.TARGET_ATTRIBUTES.getValue());
        runner.setProperty(GetS3ObjectTags.ATTRIBUTE_INCLUDE_PATTERN, "");

        final List<Tag> tags = setupObjectTags();

        run();
        runner.assertTransferCount(GetS3ObjectTags.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetS3ObjectTags.REL_FOUND).getFirst();
        tags.forEach(tag -> {
            final String key = String.format("s3.tag.%s", tag.getKey());
            final String val = flowFile.getAttribute(key);
            assertEquals(tag.getValue(), val);
        });
    }

    @DisplayName("Validate attribute exclusion")
    @Test
    void testFetchTagsToAttributeExclusion() {
        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, GetS3ObjectTags.TARGET_ATTRIBUTES.getValue());
        runner.setProperty(GetS3ObjectTags.ATTRIBUTE_INCLUDE_PATTERN, "(raw|user)");

        final List<Tag> tags = setupObjectTags();
        final List<Tag> mustHave = tags.stream().filter(tag -> !"mightHaveTo".equals(tag.getKey())).toList();

        run();
        runner.assertTransferCount(GetS3ObjectTags.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetS3ObjectTags.REL_FOUND).getFirst();
        mustHave.forEach(tag -> {
            final String key = String.format("s3.tag.%s", tag.getKey());
            final String val = flowFile.getAttribute(key);
            assertEquals(tag.getValue(), val);
        });

        assertNull(flowFile.getAttribute("s3.tag.mightHaveTo"));
    }

    @DisplayName("Validate fetch to attribute mode routes to failure on S3 error")
    @Test
    void testFetchTagsToAttributeS3Error() {
        final AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(501);

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, GetS3ObjectTags.TARGET_ATTRIBUTES.getValue());
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_FAILURE, 1);
    }

    @DisplayName("Validate fetch tags to attribute routes to not-found when when the file doesn't exist")
    @Test
    void testFetchTagsToAttributeNotExist() {
        final AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(404);

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, GetS3ObjectTags.TARGET_ATTRIBUTES.getValue());
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_NOT_FOUND, 1);
    }

    @DisplayName("Validate fetch tags to FlowFile body routes to found when the file exists")
    @Test
    void testFetchTagsToBodyExists() {
        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, GetS3ObjectTags.TARGET_FLOWFILE_BODY.getValue());
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(mockTags);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_FOUND, 1);
    }

    @DisplayName("Validate fetch tags to FlowFile body routes to not-found when when the file doesn't exist")
    @Test
    void testFetchTagsToBodyNotExist() {
        final AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(404);

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, GetS3ObjectTags.TARGET_FLOWFILE_BODY.getValue());
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_NOT_FOUND, 1);
    }

    @DisplayName("Validate fetch to FlowFile body mode routes to failure on S3 error")
    @Test
    void testFetchTagsToBodyS3Error() {
        final AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(501);

        runner.setProperty(GetS3ObjectTags.TAGS_TARGET, GetS3ObjectTags.TARGET_FLOWFILE_BODY.getValue());
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenThrow(exception);
        run();

        runner.assertTransferCount(GetS3ObjectTags.REL_FAILURE, 1);
    }
}
