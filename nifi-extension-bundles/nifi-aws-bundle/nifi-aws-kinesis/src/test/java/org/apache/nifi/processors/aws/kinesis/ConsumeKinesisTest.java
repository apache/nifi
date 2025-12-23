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
package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.PROCESSING_STRATEGY;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.ProcessingStrategy.FLOW_FILE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.ProcessingStrategy.RECORD;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.REL_PARSE_FAILURE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

class ConsumeKinesisTest {

    private TestRunner testRunner;

    @BeforeEach
    void setUp() {
        testRunner = createTestRunner();
    }

    @Test
    void getRelationshipsForFlowFileProcessingStrategy() {
        testRunner.setProperty(PROCESSING_STRATEGY, FLOW_FILE);

        final Set<Relationship> relationships = testRunner.getProcessor().getRelationships();

        assertEquals(Set.of(REL_SUCCESS), relationships);
    }

    @Test
    void getRelationshipsForRecordProcessingStrategy() {
        testRunner.setProperty(PROCESSING_STRATEGY, RECORD);

        final Set<Relationship> relationships = testRunner.getProcessor().getRelationships();

        assertEquals(Set.of(REL_SUCCESS, REL_PARSE_FAILURE), relationships);
    }

    @Test
    // It takes around 30 seconds for a scheduler to fail in this test.
    @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
    void failInitializationWithInvalidValues() {
        // KCL Scheduler initialization will fail, as the runner is configured with placeholder credentials.

        // Using the processor object to avoid error wrapping by testRunner.
        final ConsumeKinesis consumeKinesis = (ConsumeKinesis) testRunner.getProcessor();

        final ProcessContext context = testRunner.getProcessContext();
        final ProcessSession session = new MockProcessSession(new SharedSessionState(consumeKinesis, new AtomicLong()), consumeKinesis);

        final ProcessException ex = assertThrows(
                ProcessException.class,
                () -> {
                    // The error might occur either in @OnScheduled...
                    consumeKinesis.setup(context);
                    while (true) {
                        // ... or in onTrigger, if the initialization takes longer.
                        consumeKinesis.onTrigger(context, session);
                        Thread.sleep(Duration.ofSeconds(1));
                    }
                });

        assertNotNull(ex.getCause(), "The initialization exception is expected to have a cause");
    }

    private static TestRunner createTestRunner() {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesis.class);

        final AWSCredentialsProviderControllerService credentialsService = new AWSCredentialsProviderControllerService();
        try {
            runner.addControllerService("credentials", credentialsService);
        } catch (final InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, "123");
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.SECRET_KEY, "123");
        runner.enableControllerService(credentialsService);

        runner.setProperty(ConsumeKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials");
        runner.setProperty(ConsumeKinesis.STREAM_NAME, "stream");
        runner.setProperty(ConsumeKinesis.APPLICATION_NAME, "application");
        runner.setProperty(RegionUtil.REGION, "us-west-2");
        runner.setProperty(ConsumeKinesis.INITIAL_STREAM_POSITION, ConsumeKinesis.InitialPosition.TRIM_HORIZON);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, ConsumeKinesis.ProcessingStrategy.FLOW_FILE);

        runner.setProperty(ConsumeKinesis.METRICS_PUBLISHING, ConsumeKinesis.MetricsPublishing.CLOUDWATCH);

        runner.setProperty(ConsumeKinesis.MAX_BYTES_TO_BUFFER, "10 MB");

        return runner;
    }
}
