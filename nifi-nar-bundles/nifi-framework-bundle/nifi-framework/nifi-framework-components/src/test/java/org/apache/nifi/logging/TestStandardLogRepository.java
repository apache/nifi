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

package org.apache.nifi.logging;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.repository.StandardLogRepository;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestStandardLogRepository {

    @Test
    public void testLogRepository() {
        StandardLogRepository repo = new StandardLogRepository();
        MockLogObserver observer = new MockLogObserver();
        repo.addObserver(LogLevel.DEBUG, observer);

        IOException exception = new IOException("exception");

        repo.addLogMessage(LogLevel.DEBUG, "Testing {} to get exception message <{}>", new Object[]{observer.getClass().getName(), exception});
        repo.addLogMessage(LogLevel.DEBUG, "Testing {} to get exception message", new Object[]{observer.getClass().getName()}, exception);

        assertEquals("Testing org.apache.nifi.logging.TestStandardLogRepository$MockLogObserver to get exception message <exception>", observer.getMessages().get(0).getMessage());
        assertEquals("Testing org.apache.nifi.logging.TestStandardLogRepository$MockLogObserver to get exception message", observer.getMessages().get(1).getMessage());
    }

    @Test
    public void testLogRepositoryLogsFirstFlowFileUuid() {
        StandardLogRepository repo = new StandardLogRepository();
        MockLogObserver observer = new MockLogObserver();
        repo.addObserver(LogLevel.DEBUG, observer);
        MockFlowFile mockFlowFile = new MockFlowFile(1L);

        repo.addLogMessage(LogLevel.INFO, "Testing {} being shown in exception message", new Object[]{mockFlowFile});

        assertEquals(mockFlowFile.getAttribute(CoreAttributes.UUID.key()), observer.getMessages().get(0).getFlowFileUuid());
    }

    @Test
    public void testLogRepositoryDoesntLogMultipleFlowFileUuids() {
        StandardLogRepository repo = new StandardLogRepository();
        MockLogObserver observer = new MockLogObserver();
        repo.addObserver(LogLevel.DEBUG, observer);
        MockFlowFile mockFlowFile1 = new MockFlowFile(1L);
        MockFlowFile mockFlowFile2 = new MockFlowFile(2L);

        repo.addLogMessage(LogLevel.INFO, "Testing {} {} FlowFiles are not being shown in exception message", new Object[]{mockFlowFile1, mockFlowFile2});

        assertNull(observer.getMessages().get(0).getFlowFileUuid());
    }

    @Test
    public void testLogRepositoryAfterLogLevelChange() {
        StandardLogRepository repo = new StandardLogRepository();
        MockLogObserver observer = new MockLogObserver();
        repo.addObserver(LogLevel.ERROR, observer);

        repo.setObservationLevel(LogLevel.ERROR);

        IOException exception = new IOException("exception");

        repo.addLogMessage(LogLevel.ERROR, "Testing {} to get exception message <{}>", new Object[]{observer.getClass().getName(), exception});

        assertEquals(1, observer.getMessages().size());
        assertEquals("Testing org.apache.nifi.logging.TestStandardLogRepository$MockLogObserver to get exception message <exception>", observer.getMessages().get(0).getMessage());
    }

    private static class MockLogObserver implements LogObserver {
        private final List<LogMessage> messages = new ArrayList<>();

        @Override
        public void onLogMessage(LogMessage message) {
            messages.add(message);
        }

        @Override
        public String getComponentDescription() {
            return "MockLogObserver";
        }

        public List<LogMessage> getMessages() {
            return messages;
        }
    }
}