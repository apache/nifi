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

import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPutKinesisFirehose {
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
    public void testWithSizeGreaterThan1MB() {
        runner.setProperty(PutKinesisFirehose.BATCH_SIZE, "1");
        runner.assertValid();
        byte [] bytes = new byte[(PutKinesisFirehose.MAX_MESSAGE_SIZE + 1)];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisFirehose.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisFirehose.REL_FAILURE);

        assertNotNull(flowFiles.get(0).getAttribute(PutKinesisFirehose.AWS_KINESIS_FIREHOSE_ERROR_MESSAGE));
    }
}
