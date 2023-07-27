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
import org.apache.nifi.processors.aws.kinesis.stream.record.AbstractKinesisRecordProcessor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY;

public abstract class ITConsumeKinesisStream {

    static final String KINESIS_STREAM_NAME = "test-stream";
    static final String APPLICATION_NAME = "test-application";
    static final String REGION = System.getProperty("AWS_DEFAULT_REGION", Regions.US_EAST_1.getName());

    protected TestRunner runner;

    KinesisClient kinesis;
    DynamoDbClient dynamoDB;

    @Test
    public void readHorizon() throws InterruptedException, IOException {
        String partitionKey = "1";

        final PutRecordRequest request = PutRecordRequest.builder()
                .streamName(KINESIS_STREAM_NAME)
                .data(SdkBytes.fromString("horizon", Charset.defaultCharset()))
                .partitionKey(partitionKey)
                .build();
        kinesis.putRecord(request);

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

        final PutRecordRequest request = PutRecordRequest.builder()
                .streamName(KINESIS_STREAM_NAME)
                .data(SdkBytes.fromString("horizon", Charset.defaultCharset()))
                .partitionKey(partitionKey)
                .build();
        kinesis.putRecord(request);

        startKCL(runner, InitialPositionInStream.LATEST);

        final PutRecordRequest request2 = PutRecordRequest.builder()
                .streamName(KINESIS_STREAM_NAME)
                .data(SdkBytes.fromString("latest", Charset.defaultCharset()))
                .partitionKey(partitionKey)
                .build();
        kinesis.putRecord(request2);

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

    @AfterEach
    public void tearDown() throws InterruptedException, ExecutionException {
        cleanupKinesis();
        cleanupDynamoDB();

        runner = null;

        Thread.sleep(2_000);

        System.clearProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY);
    }

    private void cleanupDynamoDB() {
        if (dynamoDB != null) {
            ListTablesResponse tableResults = dynamoDB.listTables();
            List<String> tableName = tableResults.tableNames();
            if (tableName.contains(APPLICATION_NAME)) {
                dynamoDB.deleteTable(DeleteTableRequest.builder().tableName(APPLICATION_NAME).build());
            }
        }
        dynamoDB = null;
    }

    private void cleanupKinesis() {
        if (kinesis != null) {
            final ListStreamsResponse streamsResult = kinesis.listStreams();
            List<String> streamNames = streamsResult.streamNames();
            if (streamNames.contains(KINESIS_STREAM_NAME)) {
                kinesis.deleteStream(DeleteStreamRequest.builder().streamName(KINESIS_STREAM_NAME).build());
            }
        }
        kinesis = null;
    }
}
