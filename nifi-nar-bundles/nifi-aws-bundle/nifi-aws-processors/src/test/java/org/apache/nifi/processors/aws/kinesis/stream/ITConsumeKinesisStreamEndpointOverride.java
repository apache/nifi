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

import com.amazonaws.client.builder.AwsClientBuilder;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;

import java.net.URI;

import static com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY;

public class ITConsumeKinesisStreamEndpointOverride extends ITConsumeKinesisStream {

    private static final String ACCESS_KEY = "test";
    private static final String SECRET_KEY = "test";
    private static final String KINESIS_STREAM_NAME = "test-stream";
    private static final String APPLICATION_NAME = "test-application";
    private static final String LOCAL_STACK_KINESIS_ENDPOINT_OVERRIDE = "http://localhost:4566";
    private static final String LOCAL_STACK_DYNAMODB_ENDPOINT_OVERRIDE = "http://localhost:4566";

    private final AwsCredentialsProvider awsCredentialsProvider =
            StaticCredentialsProvider.create(new AwsCredentials() {
                @Override
                public String accessKeyId() {
                    return ACCESS_KEY;
                }

                @Override
                public String secretAccessKey() {
                    return SECRET_KEY;
                }
            });

    private final AwsClientBuilder.EndpointConfiguration dynamoDBEndpointConfig =
            new AwsClientBuilder.EndpointConfiguration(LOCAL_STACK_DYNAMODB_ENDPOINT_OVERRIDE, REGION);

    @BeforeEach
    public void setUp() throws InterruptedException {
        System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

        kinesis = KinesisClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .endpointOverride(URI.create(LOCAL_STACK_KINESIS_ENDPOINT_OVERRIDE))
                .httpClient(ApacheHttpClient.create())
                .region(Region.of(REGION))
                .build();

        kinesis.createStream(CreateStreamRequest.builder().streamName(KINESIS_STREAM_NAME).shardCount(1).build());

        dynamoDB = DynamoDbClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .endpointOverride(URI.create(LOCAL_STACK_DYNAMODB_ENDPOINT_OVERRIDE))
                .region(Region.of(REGION))
                .httpClient(ApacheHttpClient.create())
                .build();

        waitForKinesisToInitialize();

        runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);
        runner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, APPLICATION_NAME);
        runner.setProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME, KINESIS_STREAM_NAME);
        runner.setProperty(ConsumeKinesisStream.ACCESS_KEY, ACCESS_KEY);
        runner.setProperty(ConsumeKinesisStream.SECRET_KEY, SECRET_KEY);
        runner.setProperty(ConsumeKinesisStream.REGION, REGION);
        runner.setProperty(ConsumeKinesisStream.REPORT_CLOUDWATCH_METRICS, "false");
        runner.setProperty(ConsumeKinesisStream.ENDPOINT_OVERRIDE, LOCAL_STACK_KINESIS_ENDPOINT_OVERRIDE + "/kinesis");
        runner.setProperty(ConsumeKinesisStream.DYNAMODB_ENDPOINT_OVERRIDE, LOCAL_STACK_DYNAMODB_ENDPOINT_OVERRIDE + "/dynamodb");
        runner.assertValid();
    }

    private void waitForKinesisToInitialize() throws InterruptedException {
        Thread.sleep(1000);
    }
}
