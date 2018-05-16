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

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Provides integration level testing with actual AWS S3 resources for {@link FetchS3Object} and requires additional configuration and resources to work.
 */
public class ITFetchS3Object extends AbstractS3IT {
    @Test
    public void testSimpleGet() throws IOException {
        putTestFile("test-file", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = TestRunners.newTestRunner(new FetchS3Object());

        runner.setProperty(FetchS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(FetchS3Object.REGION, REGION);
        runner.setProperty(FetchS3Object.BUCKET, BUCKET_NAME);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeNotExists(PutS3Object.S3_SSE_ALGORITHM);
        ff.assertContentEquals(getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
    }

    @Test
    public void testSimpleGetEncrypted() throws IOException {
        putTestFileEncrypted("test-file", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = TestRunners.newTestRunner(new FetchS3Object());

        runner.setProperty(FetchS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(FetchS3Object.REGION, REGION);
        runner.setProperty(FetchS3Object.BUCKET, BUCKET_NAME);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        MockFlowFile ff = ffs.get(0);
        ff.assertAttributeEquals(PutS3Object.S3_SSE_ALGORITHM, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        ff.assertContentEquals(getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
    }

    @Test
    public void testFetchS3ObjectUsingCredentialsProviderService() throws Throwable {
        putTestFile("test-file", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = TestRunners.newTestRunner(new FetchS3Object());

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AbstractAWSProcessor.CREDENTIALS_FILE, System.getProperty("user.home") + "/aws-credentials.properties");
        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        runner.setProperty(FetchS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
        runner.setProperty(FetchS3Object.REGION, REGION);
        runner.setProperty(FetchS3Object.BUCKET, BUCKET_NAME);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test-file");
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);

    }

    @Test
    public void testTryToFetchNotExistingFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new FetchS3Object());

        runner.setProperty(FetchS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(FetchS3Object.REGION, REGION);
        runner.setProperty(FetchS3Object.BUCKET, BUCKET_NAME);

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

        final TestRunner runner = TestRunners.newTestRunner(new FetchS3Object());

        runner.setProperty(FetchS3Object.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(FetchS3Object.REGION, REGION);
        runner.setProperty(FetchS3Object.BUCKET, BUCKET_NAME);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", key);
        runner.enqueue(new byte[0], attrs);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(FetchS3Object.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        final byte[] expectedBytes = Files.readAllBytes(getResourcePath(SAMPLE_FILE_RESOURCE_NAME));
        out.assertContentEquals(new String(expectedBytes));

        for (final Map.Entry<String, String> entry : out.getAttributes().entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

}
