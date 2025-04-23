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
package org.apache.nifi.kafka.service.aws;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AmazonMSKConnectionServiceTest {

    private static final String SERVICE_ID = AmazonMSKConnectionService.class.getName();

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
    }

    @Test
    void testValidProperties() throws InitializationException {
        final AmazonMSKConnectionService service = new AmazonMSKConnectionService();
        runner.addControllerService(SERVICE_ID, service);

        runner.setProperty(service, AmazonMSKConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);

        runner.assertValid(service);

        runner.enableControllerService(service);
    }
}
