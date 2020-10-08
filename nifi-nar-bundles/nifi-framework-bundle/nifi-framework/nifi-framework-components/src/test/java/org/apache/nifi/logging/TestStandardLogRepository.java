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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.logging.repository.StandardLogRepository;
import org.junit.Test;

public class TestStandardLogRepository {

    @Test
    public void testLogRepository() {
        StandardLogRepository repo = new StandardLogRepository();
        MockLogObserver observer = new MockLogObserver();
        repo.addObserver("mock", LogLevel.DEBUG, observer);

        IOException exception = new IOException("exception");

        repo.addLogMessage(LogLevel.DEBUG, "Testing {} to get exception message <{}>", new Object[]{observer.getClass().getName(), exception});
        repo.addLogMessage(LogLevel.DEBUG, "Testing {} to get exception message", new Object[]{observer.getClass().getName()}, exception);

        assertEquals(observer.getMessages().get(0), "Testing org.apache.nifi.logging.TestStandardLogRepository$MockLogObserver to get exception message <exception>");
        assertEquals(observer.getMessages().get(1), "Testing org.apache.nifi.logging.TestStandardLogRepository$MockLogObserver to get exception message");
    }

    private class MockLogObserver implements LogObserver {

        private List<String> messages = new ArrayList<String>();

        @Override
        public void onLogMessage(LogMessage message) {
            messages.add(message.getMessage());
        }

        public List<String> getMessages() {
            return messages;
        }

    }

}