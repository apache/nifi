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

import org.apache.nifi.processors.aws.credentials.provider.PropertiesCredentialsProvider;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;

import java.io.File;

public class ITConsumeKinesisStreamConnectAWS extends ITConsumeKinesisStream {

    private final static File CREDENTIALS_FILE = new File(System.getProperty("user.home") + "/aws-credentials.properties");

    @BeforeEach
    public void setUp() throws InterruptedException, InitializationException {
        Assumptions.assumeTrue(CREDENTIALS_FILE.exists());

        System.setProperty("aws.cborEnabled", "false");

        kinesis = KinesisClient.builder()
                .credentialsProvider(new PropertiesCredentialsProvider(CREDENTIALS_FILE))
                .httpClient(ApacheHttpClient.create())
                .region(Region.of(REGION))
                .build();

        kinesis.createStream(CreateStreamRequest.builder().streamName(KINESIS_STREAM_NAME).shardCount(1).build());

        dynamoDB = DynamoDbClient.builder()
                .credentialsProvider(new PropertiesCredentialsProvider(CREDENTIALS_FILE))
                .region(Region.of(REGION))
                .httpClient(ApacheHttpClient.create())
                .build();

        waitForKinesisToInitialize();

        runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);
        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE.getAbsolutePath());
        runner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, APPLICATION_NAME);
        runner.setProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME, KINESIS_STREAM_NAME);
        runner.setProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials-service");
        runner.setProperty(ConsumeKinesisStream.REGION, REGION);
        runner.setProperty(ConsumeKinesisStream.REPORT_CLOUDWATCH_METRICS, "false");
        runner.assertValid();
    }

    private void waitForKinesisToInitialize() throws InterruptedException {
        Thread.sleep(20_000);
    }
}
