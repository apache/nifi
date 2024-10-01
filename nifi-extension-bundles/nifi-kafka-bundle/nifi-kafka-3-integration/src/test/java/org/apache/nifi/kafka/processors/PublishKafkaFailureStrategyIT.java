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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.kafka.shared.property.FailureStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PublishKafkaFailureStrategyIT extends AbstractPublishKafkaIT {
    private static final String TEST_RESOURCE = "org/apache/nifi/kafka/processors/publish/ff.not.json";

    @Test
    public void testProduceRouteToFailure() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        addRecordReaderService(runner);
        addRecordWriterService(runner);

        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.FAILURE_STRATEGY, FailureStrategy.ROUTE_TO_FAILURE);

        // attempt to send a non-json FlowFile to Kafka using record strategy;
        // this will fail on the record parsing step prior to send; triggering the failure strategy logic
        final Map<String, String> attributes = new HashMap<>();
        final byte[] bytesFlowFile = IOUtils.toByteArray(Objects.requireNonNull(getClass().getClassLoader().getResource(TEST_RESOURCE)));
        runner.enqueue(bytesFlowFile, attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);
    }

    @Test
    public void testProduceRollback() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        addRecordReaderService(runner);
        addRecordWriterService(runner);
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.FAILURE_STRATEGY, FailureStrategy.ROLLBACK);

        // attempt to send a non-json FlowFile to Kafka using record strategy;
        // this will fail on the record parsing step prior to send; triggering the failure strategy logic
        final Map<String, String> attributes = new HashMap<>();
        final byte[] bytesFlowFile = IOUtils.toByteArray(Objects.requireNonNull(getClass().getClassLoader().getResource(TEST_RESOURCE)));
        runner.enqueue(bytesFlowFile, attributes);
        runner.run(1);

        // on rollback, FlowFile is returned to source queue
        runner.assertTransferCount(PublishKafka.REL_SUCCESS, 0);
        runner.assertTransferCount(PublishKafka.REL_FAILURE, 0);
    }
}
