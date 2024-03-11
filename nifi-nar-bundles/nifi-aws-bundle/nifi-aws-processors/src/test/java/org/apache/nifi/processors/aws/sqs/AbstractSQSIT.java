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

package org.apache.nifi.processors.aws.sqs;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.v2.AbstractAwsProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractSQSIT {
    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:latest");

    private static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.SQS);

    private static String queueUrl;
    private static SqsClient client;

    @BeforeAll
    public static void setup() throws InterruptedException {
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");
        localstack.start();

        client = SqsClient.builder()
            .endpointOverride(localstack.getEndpoint())
            .credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                    )
            )
            .region(Region.of(localstack.getRegion()))
            .build();

        final CreateQueueResponse response = client.createQueue(CreateQueueRequest.builder()
                .queueName("SqsSystemTest")
                .build());
        assertTrue(response.sdkHttpResponse().isSuccessful());
        queueUrl = response.queueUrl();
    }

    @AfterAll
    public static void shutdown() {
        client.close();
        localstack.stop();
    }

    protected SqsClient getClient() {
        return client;
    }

    protected String getQueueUrl() {
        return queueUrl;
    }

    protected TestRunner initRunner(final Class<? extends Processor> processorClass) {
        TestRunner runner = TestRunners.newTestRunner(processorClass);
        AuthUtils.enableAccessKey(runner, localstack.getAccessKey(), localstack.getSecretKey());

        runner.setProperty(AbstractAwsProcessor.REGION, localstack.getRegion());
        runner.setProperty(AbstractAwsProcessor.ENDPOINT_OVERRIDE, localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString());
        runner.setProperty("Queue URL", queueUrl);
        return runner;
    }

}
