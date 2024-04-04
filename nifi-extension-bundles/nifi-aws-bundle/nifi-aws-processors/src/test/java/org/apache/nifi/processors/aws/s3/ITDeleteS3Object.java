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

import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


/**
 * Provides integration level testing with actual AWS S3 resources for {@link DeleteS3Object} and requires additional configuration and resources to work.
 */
public class ITDeleteS3Object extends AbstractS3IT {

    @Test
    public void testSimpleDelete() {
        // Prepares for this test
        putTestFile("delete-me", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(DeleteS3Object.class);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "delete-me");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteFolder() {
        // Prepares for this test
        putTestFile("folder/delete-me", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(DeleteS3Object.class);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/delete-me");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteFolderUsingCredentialsProviderService() {
        // Prepares for this test
        putTestFile("folder/delete-me", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(DeleteS3Object.class);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "folder/delete-me");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteFolderNoExpressionLanguage() {
        // Prepares for this test
        putTestFile("folder/delete-me", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(DeleteS3Object.class);
        runner.setProperty(DeleteS3Object.KEY, "folder/delete-me");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "a-different-name");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

    @Test
    public void testTryToDeleteNotExistingFile() {
        final TestRunner runner = initRunner(DeleteS3Object.class);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "no-such-a-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteS3Object.REL_SUCCESS, 1);
    }

}
