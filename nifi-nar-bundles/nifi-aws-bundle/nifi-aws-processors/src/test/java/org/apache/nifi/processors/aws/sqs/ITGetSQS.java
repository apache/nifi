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

import java.util.List;

import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.sns.PutSNS;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For local testing only - interacts with S3 so the credentials file must be configured and all necessary buckets created")
public class ITGetSQS {

    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private final String QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/100515378163/test-queue-000000000";

    @Test
    public void testSimpleGet() {
        final TestRunner runner = TestRunners.newTestRunner(new GetSQS());
        runner.setProperty(PutSNS.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(GetSQS.TIMEOUT, "30 secs");
        runner.setProperty(GetSQS.QUEUE_URL, QUEUE_URL);

        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        for (final MockFlowFile mff : flowFiles) {
            System.out.println(mff.getAttributes());
            System.out.println(new String(mff.toByteArray()));
        }
    }

    @Test
    public void testSimpleGetWithEL() {
        System.setProperty("test-account-property", "100515378163");
        System.setProperty("test-queue-property", "test-queue-000000000");
        final TestRunner runner = TestRunners.newTestRunner(new GetSQS());
        runner.setProperty(PutSNS.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(GetSQS.TIMEOUT, "30 secs");
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/${test-account-property}/${test-queue-property}");

        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        for (final MockFlowFile mff : flowFiles) {
            System.out.println(mff.getAttributes());
            System.out.println(new String(mff.toByteArray()));
        }
    }

    @Test
    public void testSimpleGetUsingCredentialsProviderService() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(new GetSQS());

        runner.setProperty(GetSQS.TIMEOUT, "30 secs");
        runner.setProperty(GetSQS.QUEUE_URL, QUEUE_URL);

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AbstractAWSProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        runner.setProperty(GetSQS.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        for (final MockFlowFile mff : flowFiles) {
            System.out.println(mff.getAttributes());
            System.out.println(new String(mff.toByteArray()));
        }
    }

}
