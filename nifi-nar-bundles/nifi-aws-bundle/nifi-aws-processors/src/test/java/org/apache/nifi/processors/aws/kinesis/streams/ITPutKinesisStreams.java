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
package org.apache.nifi.processors.aws.kinesis.streams;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This test contains both unit and integration test (integration tests are ignored by default)
 */
public class ITPutKinesisStreams {

    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisStreams.class);
        runner.setProperty(PutKinesisStreams.ACCESS_KEY, "abcd");
        runner.setProperty(PutKinesisStreams.SECRET_KEY, "secret key");
        runner.setProperty(PutKinesisStreams.KINESIS_STREAMS_DELIVERY_STREAM_NAME, "deliveryName");
        runner.assertValid();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testIntegrationSuccess() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisStreams.class);
        runner.setProperty(PutKinesisStreams.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutKinesisStreams.KINESIS_STREAMS_DELIVERY_STREAM_NAME, "testkinesis");
        runner.assertValid();

        runner.enqueue("test".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisStreams.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStreams.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        out.assertContentEquals("test".getBytes());
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testIntegrationFailedBadStreamName() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisStreams.class);
        runner.setProperty(PutKinesisStreams.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutKinesisStreams.KINESIS_STREAMS_DELIVERY_STREAM_NAME, "bad-firehose-s3-test");
        runner.assertValid();

        runner.enqueue("test".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisStreams.REL_FAILURE, 1);

    }

    @Test
    public void testOneMessageWithMaxBufferSizeEqualTo1MBOneSuccess() {
        runner = TestRunners.newTestRunner(PutKinesisStreams.class);
        runner.setProperty(PutKinesisStreams.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutKinesisStreams.NR_SHARDS, "1");
        runner.setProperty(PutKinesisStreams.KINESIS_STREAMS_DELIVERY_STREAM_NAME, "testkinesis");
        runner.assertValid();
        byte[] bytes = new byte[(PutKinesisStreams.MAX_MESSAGE_SIZE)];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisStreams.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisStreams.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
    }
}
