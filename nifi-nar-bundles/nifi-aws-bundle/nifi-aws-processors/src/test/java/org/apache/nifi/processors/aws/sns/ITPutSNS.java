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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

/**
 * Provides integration level testing with actual AWS S3 resources for {@link PutSNS} and requires additional configuration and resources to work.
 */
public class ITPutSNS {

    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Test
    public void testPublish() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutSNS());
        runner.setProperty(PutSNS.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutSNS.ARN, "arn:aws:sns:us-west-2:100515378163:test-topic-1");
        assertTrue(runner.setProperty("DynamicProperty", "hello!").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
    }

    @Test
    public void testPublishWithCredentialsProviderService() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(new PutSNS());
        String snsArn = "Add Sns arn here";
        runner.setProperty(PutSNS.ARN, snsArn);
        assertTrue(runner.setProperty("DynamicProperty", "hello!").isValid());
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AbstractAWSCredentialsProviderProcessor.CREDENTIALS_FILE, System.getProperty("user.home") + "/aws-credentials.properties");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        runner.setProperty(PutSNS.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");

        runner.run(1);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
    }
}
