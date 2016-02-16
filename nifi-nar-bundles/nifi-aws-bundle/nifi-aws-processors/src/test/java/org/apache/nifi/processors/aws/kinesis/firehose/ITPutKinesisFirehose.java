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

import java.util.List;

import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This test contains both unit and integration test (integration tests are ignored by default)
 */
public class ITPutKinesisFirehose {

    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisFirehose.class);
        runner.setProperty(PutKinesisFirehose.ACCESS_KEY, "abcd");
        runner.setProperty(PutKinesisFirehose.SECRET_KEY, "secret key");
        runner.setProperty(PutKinesisFirehose.KINESIS_FIREHOSE_DELIVERY_STREAM_NAME, "deliveryName");
        runner.assertValid();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testCustomValidateBatchSize1Valid() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "1");
        runner.assertValid();
    }

    @Test
    public void testCustomValidateBatchSize500Valid() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "500");
        runner.assertValid();
    }
    @Test
    public void testCustomValidateBatchSize501InValid() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "501");
        runner.assertNotValid();
    }

    @Test
    public void testCustomValidateBufferSize1Valid() {
        runner.setProperty(PutKinesisFirehose.MAX_BUFFER_SIZE, "1");
        runner.assertValid();
    }

    @Test
    public void testCustomValidateBufferSize128Valid() {
        runner.setProperty(PutKinesisFirehose.MAX_BUFFER_SIZE, "128");
        runner.assertValid();
    }
    @Test
    public void testCustomValidateBufferSize129InValid() {
        runner.setProperty(PutKinesisFirehose.MAX_BUFFER_SIZE, "129");
        runner.assertNotValid();
    }

    @Test
    public void testCustomValidateBufferInterval900Valid() {
        runner.setProperty(PutKinesisFirehose.MAX_BUFFER_INTERVAL, "900");
        runner.assertValid();
    }

    @Test
    public void testCustomValidateBufferInterval60Valid() {
        runner.setProperty(PutKinesisFirehose.MAX_BUFFER_INTERVAL, "60");
        runner.assertValid();
    }

    @Test
    public void testCustomValidateBufferInterval901InValid() {
        runner.setProperty(PutKinesisFirehose.MAX_BUFFER_INTERVAL, "901");
        runner.assertNotValid();
    }

    @Test
    public void testCustomValidateBufferInterval59InValid() {
        runner.setProperty(PutKinesisFirehose.MAX_BUFFER_INTERVAL, "59");
        runner.assertNotValid();
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    @Ignore
    public void testIntegrationSuccess() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisFirehose.class);
        runner.setProperty(PutKinesisFirehose.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutKinesisFirehose.KINESIS_FIREHOSE_DELIVERY_STREAM_NAME, "firehose-s3-test");
        runner.assertValid();

        runner.enqueue("test".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(FetchS3Object.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        out.assertContentEquals("test".getBytes());
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    @Ignore
    public void testIntegrationFailedBadStreamName() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisFirehose.class);
        runner.setProperty(PutKinesisFirehose.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutKinesisFirehose.KINESIS_FIREHOSE_DELIVERY_STREAM_NAME, "bad-firehose-s3-test");
        runner.assertValid();

        runner.enqueue("test".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_FAILURE, 1);

    }
}
