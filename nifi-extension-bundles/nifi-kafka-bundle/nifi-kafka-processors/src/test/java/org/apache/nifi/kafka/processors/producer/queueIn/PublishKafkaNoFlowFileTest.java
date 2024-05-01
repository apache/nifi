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
package org.apache.nifi.kafka.processors.producer.queueIn;

import org.apache.nifi.kafka.processors.PublishKafka;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.nifi.kafka.processors.PublishKafka.CONNECTION_SERVICE;
import static org.apache.nifi.kafka.processors.PublishKafka.TOPIC_NAME;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PublishKafkaNoFlowFileTest {

    private static final String TEST_TOPIC_NAME = "NiFi-Kafka-Events";

    private static final String SERVICE_ID = KafkaConnectionService.class.getSimpleName();

    @Mock
    KafkaConnectionService kafkaConnectionService;

    private TestRunner runner;

    @BeforeEach
    public void setRunner() {
        final PublishKafka processor = new PublishKafka();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testIncomingQueueEmpty() throws InitializationException {
        setConnectionService();
        runner.setProperty(TOPIC_NAME, TEST_TOPIC_NAME);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 0);
    }

    private void setConnectionService() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);

        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.enableControllerService(kafkaConnectionService);

        runner.setProperty(CONNECTION_SERVICE, SERVICE_ID);
    }
}
