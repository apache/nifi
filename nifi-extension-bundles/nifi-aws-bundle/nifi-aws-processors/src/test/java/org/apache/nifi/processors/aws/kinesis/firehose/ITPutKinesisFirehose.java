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
package org.apache.nifi.processors.aws.kinesis.firehose;

import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test contains both unit and integration test (integration tests are ignored by default)
 */

public class ITPutKinesisFirehose {
    private final byte[] ONE_MB = new byte[1000 * 1024];
    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @BeforeAll
    public static void testSkipping() {
        Assumptions.assumeTrue(new File(CREDENTIALS_FILE).exists());
    }

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisFirehose.class);
        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE);
        runner.setProperty(PutKinesisFirehose.KINESIS_FIREHOSE_DELIVERY_STREAM_NAME, "testkinesis");
    }


    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testIntegrationSuccess() throws Exception {
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();
        out.assertContentEquals("test");
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testIntegrationFailedBadStreamName() throws Exception {
        runner.setProperty(PutKinesisFirehose.KINESIS_FIREHOSE_DELIVERY_STREAM_NAME, "bad-firehose-s3-test");

        runner.enqueue("test");
        runner.run();
        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_FAILURE, 1);
    }

    @Test
    public void testOneMessageWithMaxBufferSizeGreaterThan1MBOneSuccess() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "2");
        runner.setProperty(PutKinesisFirehose.MAX_MESSAGE_BUFFER_SIZE_MB, "1 MB");
        runner.enqueue(ONE_MB);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 1);
    }

    @Test
    public void testTwoMessageBatch5WithMaxBufferSize1MBRunOnceTwoMessageSent() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "5");
        runner.setProperty(PutKinesisFirehose.MAX_MESSAGE_BUFFER_SIZE_MB, "2 MB");
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
    }

    @Test
    public void testThreeMessageWithBatch10MaxBufferSize1MBTRunOnceTwoMessageSent() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "10");
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
    }

    @Test
    public void testTwoMessageWithBatch10MaxBufferSize1MBTRunOnceTwoMessageSent() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "10");
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
    }

    @Test
    public void testThreeMessageWithBatch2MaxBufferSize1MBRunTwiceThreeMessageSent() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "2");
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 3);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
    }

    @Test
    public void testThreeMessageHello2MBThereWithBatch10MaxBufferSize1MBRunOnceTwoMessageSuccessOneFailed() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "10");
        runner.enqueue("hello".getBytes());
        runner.enqueue(ONE_MB);
        runner.enqueue("there".getBytes());
        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        assertEquals(2,flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }

        final List<MockFlowFile> flowFilesFailed = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_FAILURE);
        assertEquals(1,flowFilesFailed.size());
        for (final MockFlowFile flowFileFailed : flowFilesFailed) {
            flowFileFailed.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_ERROR_MESSAGE);
        }
    }

    @Test
    public void testTwoMessageHello2MBWithBatch10MaxBufferSize1MBRunOnceOneSuccessOneFailed() throws Exception {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "10");
        runner.enqueue("hello".getBytes());
        runner.enqueue(ONE_MB);
        runner.run(1, true, true);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        assertEquals(1,flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
            flowFile.assertContentEquals("hello".getBytes());
        }

        final List<MockFlowFile> flowFilesFailed = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_FAILURE);
        assertEquals(1,flowFilesFailed.size());
        for (final MockFlowFile flowFileFailed : flowFilesFailed) {
            assertNotNull(flowFileFailed.getAttribute(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_ERROR_MESSAGE));
        }
    }

    @Test
    public void testTwoMessage2MBHelloWorldWithBatch10MaxBufferSize1MBRunOnceTwoMessageSent() throws Exception {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "10");
        runner.enqueue(ONE_MB);
        runner.enqueue("HelloWorld".getBytes());
        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        assertEquals(1,flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
            flowFile.assertContentEquals("HelloWorld".getBytes());
        }

        final List<MockFlowFile> flowFilesFailed = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_FAILURE);
        assertEquals(1,flowFilesFailed.size());
        for (final MockFlowFile flowFileFailed : flowFilesFailed) {
            assertNotNull(flowFileFailed.getAttribute(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_ERROR_MESSAGE));
        }
    }

    @Test
    public void testTwoMessageHelloWorldWithBatch10MaxBufferSize1MBRunOnceTwoMessageSent() throws Exception {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "10");
        runner.enqueue("Hello".getBytes());
        runner.enqueue("World".getBytes());
        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        assertEquals(2,flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
        flowFiles.get(0).assertContentEquals("Hello".getBytes());
        flowFiles.get(1).assertContentEquals("World".getBytes());

        final List<MockFlowFile> flowFilesFailed = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_FAILURE);
        assertEquals(0,flowFilesFailed.size());
    }

    @Test
    public void testThreeMessageWithBatch5MaxBufferSize1MBRunOnceTwoMessageSent() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "5");
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        assertEquals(2,flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
    }

    @Test
    public void test5MessageWithBatch10MaxBufferSize10MBRunOnce5MessageSent() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "10");
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 5);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        assertEquals(5,flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
    }

    @Test
    public void test5MessageWithBatch2MaxBufferSize10MBRunOnce2MessageSent() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "2");
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.enqueue(ONE_MB);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_SUCCESS);
        assertEquals(2,flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_RECORD_ID);
        }
    }
}
