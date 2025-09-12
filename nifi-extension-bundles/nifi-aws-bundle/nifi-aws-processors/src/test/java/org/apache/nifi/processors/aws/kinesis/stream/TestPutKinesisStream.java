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
package org.apache.nifi.processors.aws.kinesis.stream;

import org.apache.nifi.processors.aws.kinesis.KinesisProcessorUtils;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestPutKinesisStream {
    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisStream.class);
        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, "kstream");
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
        runner.assertValid();
    }

    @AfterEach
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testCustomValidateBatchSize1Valid() {
        runner.setProperty(PutKinesisStream.BATCH_SIZE, "1");
        runner.assertValid();
    }

    @Test
    public void testCustomValidateBatchSize500Valid() {
        runner.setProperty(PutKinesisStream.BATCH_SIZE, "500");
        runner.assertValid();
    }
    @Test
    public void testCustomValidateBatchSize501InValid() {
        runner.setProperty(PutKinesisStream.BATCH_SIZE, "501");
        runner.assertNotValid();
    }

    @Test
    public void testWithSizeGreaterThan1MB() {
        runner.setProperty(PutKinesisStream.BATCH_SIZE, "1");
        runner.assertValid();
        byte[] bytes = new byte[(KinesisProcessorUtils.MAX_MESSAGE_SIZE + 1)];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisStream.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutKinesisStream.REL_FAILURE);

        assertNotNull(flowFiles.get(0).getAttribute(PutKinesisStream.AWS_KINESIS_ERROR_MESSAGE));
    }
}
