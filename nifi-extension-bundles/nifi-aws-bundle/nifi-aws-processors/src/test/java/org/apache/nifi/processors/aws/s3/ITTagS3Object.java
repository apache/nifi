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

import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.Tag;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Provides integration level testing with actual AWS S3 resources for {@link TagS3Object} and requires additional
 * configuration and resources to work.
 */
public class ITTagS3Object extends AbstractS3IT {
    private static final String TAG_KEY_NIFI = "nifi-key";
    private static final String TAG_VALUE_NIFI = "nifi-val";
    private static final String TEST_FILE = "test-file";
    private static final Tag OLD_TAG = new Tag("oldkey", "oldvalue");
    private static final Tag TAG = new Tag(TAG_KEY_NIFI, TAG_VALUE_NIFI);

    private TestRunner initRunner() {
        final TestRunner runner = initRunner(TagS3Object.class);
        runner.setProperty(TagS3Object.TAG_KEY, TAG_KEY_NIFI);
        runner.setProperty(TagS3Object.TAG_VALUE, TAG_VALUE_NIFI);
        return runner;
    }

    @Test
    public void testSimpleTag() {
        // put file in s3
        putTestFile(TEST_FILE, getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        // Set up processor
        final TestRunner runner = initRunner();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", TEST_FILE);
        runner.enqueue(new byte[0], attrs);

        // tag file
        runner.run(1);

        // Verify processor succeeds
        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        assertTagsCorrect(false);
    }

    private void assertTagsCorrect(final boolean expectOldTag) {
        // Verify tag exists on S3 object
        GetObjectTaggingResult res = getClient().getObjectTagging(new GetObjectTaggingRequest(BUCKET_NAME, TEST_FILE));
        assertTrue(res.getTagSet().contains(TAG), "Expected tag not found on S3 object");

        if (expectOldTag) {
            assertTrue(res.getTagSet().contains(OLD_TAG), "Expected existing tag not found on S3 object");
        } else {
            assertFalse(res.getTagSet().contains(OLD_TAG), "Existing tag found on S3 object");
        }
    }

    @Test
    public void testAppendTag() {
        // put file in s3
        putFileWithObjectTag(TEST_FILE, getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME), List.of(OLD_TAG));

        // Set up processor
        final TestRunner runner = initRunner();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", TEST_FILE);
        runner.enqueue(new byte[0], attrs);

        // tag file
        runner.run(1);

        // Verify processor succeeds
        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);
        assertTagsCorrect(true);
    }

    @Test
    public void testReplaceTags() {
        // put file in s3
        putFileWithObjectTag(TEST_FILE, getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME), List.of(OLD_TAG));

        // Set up processor
        final TestRunner runner = initRunner();
        runner.setProperty(TagS3Object.APPEND_TAG, "false");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", TEST_FILE);
        attrs.put("s3.tag." + OLD_TAG.getKey(), OLD_TAG.getValue());
        runner.enqueue(new byte[0], attrs);

        // tag file
        runner.run(1);

        // Verify processor succeeds
        runner.assertAllFlowFilesTransferred(TagS3Object.REL_SUCCESS, 1);

        // Verify flowfile attributes match s3 tags
        MockFlowFile flowFiles = runner.getFlowFilesForRelationship(TagS3Object.REL_SUCCESS).get(0);
        flowFiles.assertAttributeNotExists(OLD_TAG.getKey());
        flowFiles.assertAttributeEquals("s3.tag." + TAG_KEY_NIFI, TAG_VALUE_NIFI);

        assertTagsCorrect(false);
    }
}

