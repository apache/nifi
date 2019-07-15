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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;

import static com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY;

public class ITGetKinesisStreamEndpointOverride extends ITGetKinesisStream {

    private static final String ACCESS_KEY = "access key";
    private static final String SECRET_KEY = "secret key";
    private static final String KINESIS_STREAM_NAME = "test-stream";
    private static final String APPLICATION_NAME = "test-application";
    private static final String LOCAL_STACK_KINESIS_ENDPOINT_OVERRIDE = "http://localhost:4568";
    private static final String LOCAL_STACK_DYNAMODB_ENDPOINT_OVERRIDE = "http://localhost:4569";

    private AWSCredentialsProvider awsCredentialsProvider =
            new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));

    private AwsClientBuilder.EndpointConfiguration kinesisEndpointConfig =
            new AwsClientBuilder.EndpointConfiguration(LOCAL_STACK_KINESIS_ENDPOINT_OVERRIDE, REGION);

    private AwsClientBuilder.EndpointConfiguration dynamoDBEndpointConfig =
            new AwsClientBuilder.EndpointConfiguration(LOCAL_STACK_DYNAMODB_ENDPOINT_OVERRIDE, REGION);

    @Before
    public void setUp() throws InterruptedException {
        System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

        kinesis = AmazonKinesisClient.builder()
                .withCredentials(awsCredentialsProvider)
                .withEndpointConfiguration(kinesisEndpointConfig)
                .build();

        kinesis.createStream(KINESIS_STREAM_NAME, 1);

        dynamoDB = AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(dynamoDBEndpointConfig)
                .withCredentials(awsCredentialsProvider)
                .build();

        waitForKinesisToInitialize();

        runner = TestRunners.newTestRunner(GetKinesisStream.class);
        runner.setProperty(GetKinesisStream.APPLICATION_NAME, APPLICATION_NAME);
        runner.setProperty(GetKinesisStream.KINESIS_STREAM_NAME, KINESIS_STREAM_NAME);
        runner.setProperty(GetKinesisStream.ACCESS_KEY, ACCESS_KEY);
        runner.setProperty(GetKinesisStream.SECRET_KEY, SECRET_KEY);
        runner.setProperty(GetKinesisStream.REGION, REGION);
        runner.setProperty(GetKinesisStream.DISABLE_CLOUDWATCH, "true");
        runner.setProperty(GetKinesisStream.ENDPOINT_OVERRIDE, LOCAL_STACK_KINESIS_ENDPOINT_OVERRIDE);
        runner.setProperty(GetKinesisStream.DYNAMODB_ENDPOINT_OVERRIDE, LOCAL_STACK_DYNAMODB_ENDPOINT_OVERRIDE);
        runner.assertValid();
    }

    private void waitForKinesisToInitialize() throws InterruptedException {
        Thread.sleep(1000);
    }
}
