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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with SQS so the credentials file must be configured and all necessary queues created")
public class ITPutSQS {

    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private final String QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/100515378163/test-queue-000000000";
    private final String REGION = Regions.US_WEST_2.getName();

    private final String VPCE_QUEUE_URL = "https://vpce-1234567890abcdefg-12345678.sqs.us-west-2.vpce.amazonaws.com/123456789012/test-queue";
    private final String VPCE_ENDPOINT_OVERRIDE = "https://vpce-1234567890abcdefg-12345678.sqs.us-west-2.vpce.amazonaws.com";

    @Test
    public void testSimplePut() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutSQS());
        runner.setProperty(PutSQS.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutSQS.REGION, REGION);
        runner.setProperty(PutSQS.QUEUE_URL, QUEUE_URL);
        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }

    @Test
    public void testSimplePutUsingCredentialsProviderService() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(new PutSQS());

        runner.setProperty(PutSQS.REGION, REGION);
        runner.setProperty(PutSQS.QUEUE_URL, QUEUE_URL);

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CredentialPropertyDescriptors.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        runner.setProperty(PutSQS.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");

        runner.assertValid();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }

    @Test
    public void testVpceEndpoint() throws IOException {
        // additional AWS environment setup for testing VPCE endpoints:
        //   - create an Interface Endpoint in your VPC for SQS (https://docs.aws.amazon.com/vpc/latest/privatelink/vpce-interface.html#create-interface-endpoint)
        //   - create a Client VPN Endpoint in your VPC (https://docs.aws.amazon.com/vpn/latest/clientvpn-admin/cvpn-getting-started.html)
        //         and connect your local machine (running the test) to your VPC via VPN
        //   - alternatively, the test can be run on an EC2 instance located on the VPC

        final TestRunner runner = TestRunners.newTestRunner(new PutSQS());
        runner.setProperty(PutSQS.CREDENTIALS_FILE, System.getProperty("user.home") + "/aws-credentials.properties");
        runner.setProperty(PutSQS.REGION, Regions.US_WEST_2.getName());
        runner.setProperty(PutSQS.QUEUE_URL, VPCE_QUEUE_URL);
        runner.setProperty(PutSQS.ENDPOINT_OVERRIDE, VPCE_ENDPOINT_OVERRIDE);

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }
}
