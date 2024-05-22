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
package org.apache.nifi.processors.aws.sns;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.processors.aws.v2.AbstractAwsProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Provides integration level testing with actual AWS S3 resources for {@link PutSNS} and requires additional configuration and resources to work.
 */
public class ITPutSNS {

    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:latest");

    private static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.SNS);

    private static final String CREDENTIALS_FILE = "src/test/resources/mock-aws-credentials.properties";
    private static String topicARN;
    private static SnsClient client;

    @BeforeAll
    public static void setup() throws InterruptedException {
        localstack.start();

        client = SnsClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                        )
                )
                .region(Region.of(localstack.getRegion()))
                .build();

        final CreateTopicResponse response = client.createTopic(CreateTopicRequest.builder()
                .name("SnsSystemTest")
                .build());
        assertTrue(response.sdkHttpResponse().isSuccessful());
        topicARN = response.topicArn();
    }

    @AfterAll
    public static void shutdown() {
        client.close();
        localstack.stop();
    }

    @Test
    public void testPublish() throws IOException {
        final TestRunner runner = initRunner(PutSNS.class);
        AuthUtils.enableAccessKey(runner, localstack.getAccessKey(), localstack.getSecretKey());
        assertTrue(runner.setProperty("DynamicProperty", "hello!").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
    }

    @Test
    public void testPublishWithCredentialsProviderService() throws Throwable {
        final TestRunner runner = initRunner(PutSNS.class);
        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
    }

    protected TestRunner initRunner(final Class<? extends Processor> processorClass) {
        TestRunner runner = TestRunners.newTestRunner(processorClass);
        runner.setProperty(AbstractAwsProcessor.REGION, localstack.getRegion());
        runner.setProperty(AbstractAwsProcessor.ENDPOINT_OVERRIDE, localstack.getEndpointOverride(LocalStackContainer.Service.SNS).toString());
        runner.setProperty(PutSNS.ARN, topicARN);
        return runner;
    }
}