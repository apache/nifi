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
package org.apache.nifi.processors.aws.cloudwatch;

import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Provides integration level testing with actual AWS CloudWatch resources for
 * {@link PutCloudWatchMetric} and requires additional configuration and resources to work.
 */
public class ITPutCloudWatchMetric {

    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Test
    public void ifCredentialsThenTestPublish() {
        final TestRunner runner = TestRunners.newTestRunner(new PutCloudWatchMetric());
        File credsFile = new File(CREDENTIALS_FILE);
        assumeTrue(credsFile.exists());

        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE);

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "Test");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "Test");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");

        runner.enqueue(new byte[] {});
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
    }

    @Test
    public void ifCredentialsThenTestPublishWithCredentialsProviderService() {
        final TestRunner runner = TestRunners.newTestRunner(new PutCloudWatchMetric());
        File credsFile = new File(CREDENTIALS_FILE);
        assumeTrue(credsFile.exists());

        AuthUtils.enableCredentialsFile(runner, credsFile.getAbsolutePath());

        runner.setProperty(PutCloudWatchMetric.NAMESPACE, "Test");
        runner.setProperty(PutCloudWatchMetric.METRIC_NAME, "Test");
        runner.setProperty(PutCloudWatchMetric.VALUE, "1.0");

        runner.enqueue(new byte[] {});
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCloudWatchMetric.REL_SUCCESS, 1);
    }
}