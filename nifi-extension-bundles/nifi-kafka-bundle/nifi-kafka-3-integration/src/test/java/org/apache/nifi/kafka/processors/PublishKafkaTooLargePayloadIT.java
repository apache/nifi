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
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Verify expected behavior on submission of "too large" record, as specified in [#getEnvironmentIntegration()]
public class PublishKafkaTooLargePayloadIT extends AbstractPublishKafkaIT {

    private static final int MESSAGE_SIZE = 1024 * 1024 * 5 / 2;
    private static final int MESSAGE_SIZE_CONFIG = MESSAGE_SIZE + 128;  // extra space for Kafka internal buffer

    @Test
    public void testProduceLargeFlowFile_ClientFailure() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);

        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());

        runner.enqueue(new byte[MESSAGE_SIZE]);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);

        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        assertEquals(1, errorMessages.size());
        final LogMessage logMessage = errorMessages.getFirst();
        assertTrue(logMessage.getMsg().contains("IOException"));
        assertTrue(Pattern.compile("max.message.size \\d+ exceeded").matcher(logMessage.getMsg()).find());
    }

    @Test
    public void testProduceLargeFlowFile_ServerFailure() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);

        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.MAX_REQUEST_SIZE, MESSAGE_SIZE_CONFIG + "B");

        runner.enqueue(new byte[MESSAGE_SIZE]);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);

        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        assertEquals(1, errorMessages.size());
        final LogMessage logMessage = errorMessages.getFirst();
        assertTrue(logMessage.getMsg().contains("RecordTooLargeException"));
        assertTrue(logMessage.getMsg().contains("larger than the max message size the server will accept"));
    }
}
