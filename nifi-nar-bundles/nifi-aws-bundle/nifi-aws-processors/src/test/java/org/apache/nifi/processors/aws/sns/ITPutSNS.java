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

import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Provides integration level testing with actual AWS S3 resources for {@link PutSNS} and requires additional configuration and resources to work.
 */
public class ITPutSNS {

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private final String TOPIC_ARN = "Add SNS ARN here";

    @BeforeAll
    public static void assumeCredentialsFileExists() {
        Assumptions.assumeTrue(new File(CREDENTIALS_FILE).exists());
    }

    @Test
    public void testPublish() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutSNS());
        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE);
        runner.setProperty(PutSNS.ARN, TOPIC_ARN);
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
        runner.setProperty(PutSNS.ARN, TOPIC_ARN);
        assertTrue(runner.setProperty("DynamicProperty", "hello!").isValid());

        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE);
        runner.run(1);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
    }
}
