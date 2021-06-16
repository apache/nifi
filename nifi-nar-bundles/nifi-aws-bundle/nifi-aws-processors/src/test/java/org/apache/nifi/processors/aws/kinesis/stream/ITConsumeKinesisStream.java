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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import org.apache.nifi.processors.aws.kinesis.stream.record.AbstractKinesisRecordProcessor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;

import static com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY;

public abstract class ITConsumeKinesisStream {

    static final String KINESIS_STREAM_NAME = "test-stream";
    static final String APPLICATION_NAME = "test-application";
    static final String REGION = System.getProperty("AWS_DEFAULT_REGION", Regions.US_EAST_1.getName());

    protected TestRunner runner;

    AmazonKinesis kinesis;
    AmazonDynamoDB dynamoDB;

    @Test
    public void readHorizon() throws InterruptedException, IOException {
        String partitionKey = "1";

        kinesis.putRecord(KINESIS_STREAM_NAME, ByteBuffer.wrap("horizon".getBytes()), partitionKey);

        startKCL(runner, InitialPositionInStream.TRIM_HORIZON);

        runner.assertAllFlowFilesTransferred(ConsumeKinesisStream.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        out.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_PARTITION_KEY, partitionKey);

        out.assertContentEquals("horizon".getBytes());
    }

    @Test
    public void readLatest() throws InterruptedException, IOException {
        String partitionKey = "1";

        kinesis.putRecord(KINESIS_STREAM_NAME, ByteBuffer.wrap("horizon".getBytes()), partitionKey);

        startKCL(runner, InitialPositionInStream.LATEST);

        kinesis.putRecord(KINESIS_STREAM_NAME, ByteBuffer.wrap("latest".getBytes()), partitionKey);

        waitForKCLToProcessTheLatestMessage();

        runner.assertAllFlowFilesTransferred(ConsumeKinesisStream.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        out.assertAttributeEquals(AbstractKinesisRecordProcessor.AWS_KINESIS_PARTITION_KEY, partitionKey);

        out.assertContentEquals("latest".getBytes());
    }

    private void waitForKCLToProcessTheLatestMessage() throws InterruptedException {
        Thread.sleep(10_000);
    }

    private void startKCL(TestRunner runner, InitialPositionInStream initialPositionInStream) throws InterruptedException {
        runner.setProperty(ConsumeKinesisStream.INITIAL_STREAM_POSITION, initialPositionInStream.name());

        runner.assertValid();

        Executors.newSingleThreadScheduledExecutor().submit((Runnable) runner::run);

        Thread.sleep(30_000);
    }

    @After
    public void tearDown() throws InterruptedException {
        cleanupKinesis();
        cleanupDynamoDB();

        runner = null;

        Thread.sleep(2_000);

        System.clearProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY);
    }

    private void cleanupDynamoDB() {
        if (dynamoDB != null) {
            ListTablesResult tableResults = dynamoDB.listTables();
            List<String> tableName = tableResults.getTableNames();
            if (tableName.contains(APPLICATION_NAME)) {
                dynamoDB.deleteTable(APPLICATION_NAME);
            }
        }
        dynamoDB = null;
    }

    private void cleanupKinesis() {
        if (kinesis != null) {
            ListStreamsResult streamsResult = kinesis.listStreams();
            List<String> streamNames = streamsResult.getStreamNames();
            if (streamNames.contains(KINESIS_STREAM_NAME)) {
                kinesis.deleteStream(new DeleteStreamRequest().withStreamName(KINESIS_STREAM_NAME));
            }
        }
        kinesis = null;
    }
}
