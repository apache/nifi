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

import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Provides integration level testing with actual AWS S3 resources for {@link FetchS3Object} and requires additional configuration and resources to work.
 */
public class ITFetchS3Object extends AbstractS3IT {
    private static final Logger logger = LoggerFactory.getLogger(ITFetchS3Object.class);

    @Test
    public void testSimpleGet() throws IOException {
        putTestFile("test-file", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(FetchS3Object.class);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.getFirst();
        ff.assertAttributeNotExists(PutS3Object.S3_SSE_ALGORITHM);
        ff.assertContentEquals(getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
    }

    @Test
    public void testSimpleGetEncrypted() throws IOException {
        putTestFileEncrypted("test-file", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(FetchS3Object.class);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.getFirst();
        ff.assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, "AES256");
        ff.assertContentEquals(getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
    }

    @Test
    public void testFetchS3ObjectUsingCredentialsProviderService() {
        putTestFile("test-file", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(FetchS3Object.class);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);

    }

    @Test
    public void testTryToFetchNotExistingFile() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new FetchS3Object());

        setSecureProperties(runner);
        runner.setProperty(RegionUtil.REGION, getRegion());
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, BUCKET_NAME);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "no-such-a-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testContentsOfFileRetrieved() throws IOException {
        String key = "folder/1.txt";
        putTestFile(key, getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = initRunner(FetchS3Object.class);
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", key);
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        final MockFlowFile out = ffs.getFirst();

        final byte[] expectedBytes = Files.readAllBytes(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        out.assertContentEquals(new String(expectedBytes));

        for (final Map.Entry<String, String> entry : out.getAttributes().entrySet()) {
            logger.info("{}:{}", entry.getKey(), entry.getValue());
        }
    }

}
