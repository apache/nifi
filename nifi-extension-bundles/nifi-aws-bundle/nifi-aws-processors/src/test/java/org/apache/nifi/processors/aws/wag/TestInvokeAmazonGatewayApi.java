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
package org.apache.nifi.processors.aws.wag;

import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestInvokeAmazonGatewayApi extends TestInvokeAWSGatewayApiCommon {

    @BeforeEach
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(InvokeAWSGatewayApi.class);
        runner.setValidateExpressionUsage(false);
        setupControllerService();
        mockWebServer = new MockWebServer();
    }

    @AfterEach
    public void after() {
        runner.shutdown();
    }

    @Test
    public void testStaticCredentials() throws Exception {
        runner.clearProperties();

        setupAuth();
        test200();
    }

    @Test
    public void testCredentialsFile() throws Exception {
        runner.clearProperties();
        setupCredFile();
        test200();
    }
}
