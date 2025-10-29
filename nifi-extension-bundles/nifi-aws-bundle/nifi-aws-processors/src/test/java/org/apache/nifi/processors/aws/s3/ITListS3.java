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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Provides integration level testing with actual AWS S3 resources for {@link ListS3} and requires additional configuration and resources to work.
 */
public class ITListS3 extends AbstractS3IT {
    @Test
    public void testSimpleList() throws IOException, InitializationException {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "a");
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
    }

    @Test
    public void testSimpleListUsingCredentialsProviderService() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "a");
        flowFiles.get(1).assertAttributeEquals("filename", "b/c");
        flowFiles.get(2).assertAttributeEquals("filename", "d/e");
    }

    @Test
    public void testSimpleListWithDelimiter() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.DELIMITER, "/");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "a");
    }

    @Test
    public void testSimpleListWithPrefix() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "b/");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "b/c");
    }

    @Test
    public void testSimpleListWithPrefixAndVersions() throws Throwable {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "b/");
        runner.setProperty(ListS3.USE_VERSIONS, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "b/c");
    }

    @Test
    public void testObjectTagsWritten() throws InitializationException {
        List<Tag> objectTags = new ArrayList<>();
        objectTags.add(Tag.builder().key("dummytag1").value("dummyvalue1").build());
        objectTags.add(Tag.builder().key("dummytag2").value("dummyvalue2").build());

        putFileWithObjectTag("t/fileWithTag", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME), objectTags);
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "t/");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(ListS3.WRITE_OBJECT_TAGS, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);

        MockFlowFile flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);

        flowFiles.assertAttributeEquals("filename", "t/fileWithTag");
        flowFiles.assertAttributeExists("s3.tag.dummytag1");
        flowFiles.assertAttributeExists("s3.tag.dummytag2");
        flowFiles.assertAttributeEquals("s3.tag.dummytag1", "dummyvalue1");
        flowFiles.assertAttributeEquals("s3.tag.dummytag2", "dummyvalue2");
    }

    @Test
    public void testUserMetadataWritten() throws FileNotFoundException, InitializationException {
        Map<String, String> userMetadata = new HashMap<>();
        userMetadata.put("dummy.metadata.1", "dummyvalue1");
        userMetadata.put("dummy.metadata.2", "dummyvalue2");

        putFileWithUserMetadata("m/fileWithUserMetadata", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME), userMetadata);
        waitForFilesAvailable();

        final TestRunner runner = initRunner(ListS3.class);
        runner.setProperty(ListS3.PREFIX, "m/");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);
        runner.setProperty(ListS3.WRITE_USER_METADATA, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);

        MockFlowFile flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);

        flowFiles.assertAttributeEquals("filename", "m/fileWithUserMetadata");
        flowFiles.assertAttributeExists("s3.user.metadata.dummy.metadata.1");
        flowFiles.assertAttributeExists("s3.user.metadata.dummy.metadata.2");
        flowFiles.assertAttributeEquals("s3.user.metadata.dummy.metadata.1", "dummyvalue1");
        flowFiles.assertAttributeEquals("s3.user.metadata.dummy.metadata.2", "dummyvalue2");
    }

}
