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

import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


/**
 * Provides integration level testing with actual AWS S3 resources for {@link ListS3} and requires additional configuration and resources to work.
 */
public class ITListS3 extends AbstractS3IT {
    @Test
    public void testSimpleList() throws IOException {
        putTestFile("a", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("b/c", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));
        putTestFile("d/e", getFileFromResourceName(SAMPLE_FILE_RESOURCE_NAME));

        final TestRunner runner = TestRunners.newTestRunner(new ListS3());

        runner.setProperty(ListS3.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(ListS3.REGION, REGION);
        runner.setProperty(ListS3.BUCKET, BUCKET_NAME);

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

        final TestRunner runner = TestRunners.newTestRunner(new ListS3());

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AbstractAWSProcessor.CREDENTIALS_FILE, System.getProperty("user.home") + "/aws-credentials.properties");
        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        runner.setProperty(ListS3.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
        runner.setProperty(ListS3.REGION, REGION);
        runner.setProperty(ListS3.BUCKET, BUCKET_NAME);

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

        final TestRunner runner = TestRunners.newTestRunner(new ListS3());

        runner.setProperty(ListS3.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(ListS3.REGION, REGION);
        runner.setProperty(ListS3.BUCKET, BUCKET_NAME);
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

        final TestRunner runner = TestRunners.newTestRunner(new ListS3());

        runner.setProperty(ListS3.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(ListS3.REGION, REGION);
        runner.setProperty(ListS3.BUCKET, BUCKET_NAME);
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

        final TestRunner runner = TestRunners.newTestRunner(new ListS3());

        runner.setProperty(ListS3.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(ListS3.REGION, REGION);
        runner.setProperty(ListS3.BUCKET, BUCKET_NAME);
        runner.setProperty(ListS3.PREFIX, "b/");
        runner.setProperty(ListS3.USE_VERSIONS, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals("filename", "b/c");
    }

}
