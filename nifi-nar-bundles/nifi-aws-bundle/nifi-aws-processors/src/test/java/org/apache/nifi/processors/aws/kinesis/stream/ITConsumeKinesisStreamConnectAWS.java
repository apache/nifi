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

import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;

import static com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY;

public class ITConsumeKinesisStreamConnectAWS extends ITConsumeKinesisStream {

    private final static String CREDENTIALS_FILE =
            System.getProperty("user.home") + "/aws-credentials.properties";

    @Before
    public void setUp() throws InterruptedException {
        System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

        kinesis = AmazonKinesisClient.builder()
                .withCredentials(new PropertiesFileCredentialsProvider(CREDENTIALS_FILE))
                .withRegion(REGION)
                .build();

        kinesis.createStream(KINESIS_STREAM_NAME, 1);

        dynamoDB = AmazonDynamoDBClient.builder()
                .withCredentials(new PropertiesFileCredentialsProvider(CREDENTIALS_FILE))
                .withRegion(REGION)
                .build();

        waitForKinesisToInitialize();

        runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);
        runner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, APPLICATION_NAME);
        runner.setProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME, KINESIS_STREAM_NAME);
        runner.setProperty(ConsumeKinesisStream.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(ConsumeKinesisStream.REGION, REGION);
        runner.setProperty(ConsumeKinesisStream.REPORT_CLOUDWATCH_METRICS, "false");
        runner.assertValid();
    }

    private void waitForKinesisToInitialize() throws InterruptedException {
        Thread.sleep(20_000);
    }
}
