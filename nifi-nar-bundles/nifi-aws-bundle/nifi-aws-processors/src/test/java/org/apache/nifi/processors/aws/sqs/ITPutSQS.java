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
import java.util.List;
import java.util.Map;

import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.sns.PutSNS;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class ITPutSQS {

    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private final String QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/100515378163/test-queue-000000000";

    @Test
    public void testSimplePut() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutSQS());
        runner.setProperty(PutSNS.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutSQS.TIMEOUT, "30 secs");
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

        runner.setProperty(PutSQS.TIMEOUT, "30 secs");
        runner.setProperty(PutSQS.QUEUE_URL, QUEUE_URL);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AbstractAWSProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
                runner.setProperty(PutSQS.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutSQS.REL_SUCCESS);
        for (final MockFlowFile mff : flowFiles) {
            System.out.println(mff.getAttributes());
            System.out.println(new String(mff.toByteArray()));
        }

    }
}
