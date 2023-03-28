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
package org.apache.nifi.kafka.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class ConsumeKafkaServiceLifecycleIT extends AbstractConsumeKafkaIT {

    private static final String CONSUMER_GROUP_ID = ConsumeKafkaIT.class.getName();

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.GROUP_ID, CONSUMER_GROUP_ID);
    }

    /**
     * Lifecycle of consumer service should be controlled in controller service.  Processor should
     * avoid modifying controller service state.
     */
    @Test
    void testScheduleProcessorMultipleTimes() {
        runner.setProperty(ConsumeKafka.TOPICS, UUID.randomUUID().toString());
        runner.run();
        runner.run();
    }
}
