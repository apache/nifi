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
package org.apache.nifi.rules.handlers;

import junit.framework.TestCase;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestLogHandler {

    TestRunner runner;
    MockComponentLog mockComponentLog;
    LogHandler logHandler;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        mockComponentLog = new MockComponentLog();
        LogHandler handler = new MockLogHandler(mockComponentLog);
        runner.addControllerService("MockLogHandler", handler);
        runner.enableControllerService(handler);
        logHandler = (LogHandler) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("MockLogHandler");

    }

    @Test
    public void testValidService() {
        runner.assertValid(logHandler);
        assertThat(logHandler, instanceOf(LogHandler.class));
    }

    @Test
    public void testWarningLogged() {
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String expectedMessage = "--------------------------------------------------\n" +
                "Log Message: This is a warning\n" +
                "Log Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";


        attributes.put("logLevel", "warn");
        attributes.put("message", "This is a warning");
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");
        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        logHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage, logMessage);
    }

    @Test
    public void testNoLogAttributesProvided() {

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();
        final String expectedMessage = "--------------------------------------------------\n" +
                "Log Message: Rules Action Triggered Log.\n" +
                "Log Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        logHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getInfoMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage, logMessage);

    }

    @Test
    public void testInvalidLogLevelProvided() {
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        attributes.put("logLevel", "FAKE");

        final String expectedMessage = "--------------------------------------------------\n" +
                "Log Message: Rules Action Triggered Log.\n" +
                "Log Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        logHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getInfoMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage, logMessage);

    }

    @Test
    public void testInvalidActionTypeException() {
        runner.disableControllerService(logHandler);
        runner.setProperty(logHandler, AlertHandler.ENFORCE_ACTION_TYPE, "LOG");
        runner.setProperty(logHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "EXCEPTION");
        runner.enableControllerService(logHandler);

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        attributes.put("logLevel", "FAKE");

        final String expectedMessage = "--------------------------------------------------\n" +
                "Log Message: Rules Action Triggered Log.\n" +
                "Log Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes);
        try {
            logHandler.execute(action, metrics);
            fail();
        } catch (UnsupportedOperationException ex) {
            assertTrue(true);
        }
    }

    @Test
    public void testInvalidActionTypeWarning() {
        runner.disableControllerService(logHandler);
        runner.setProperty(logHandler, AlertHandler.ENFORCE_ACTION_TYPE, "LOG");
        runner.setProperty(logHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "WARN");
        runner.enableControllerService(logHandler);

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        attributes.put("logLevel", "FAKE");

        final String expectedMessage = "--------------------------------------------------\n" +
                "Log Message: Rules Action Triggered Log.\n" +
                "Log Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes);
        try {
            logHandler.execute(action, metrics);
        } catch (UnsupportedOperationException ex) {
            fail();
        }

        final String warnMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(warnMessage));
        TestCase.assertEquals("This Action Handler does not support actions with the provided type: FAKE",warnMessage);
    }

    @Test
    public void testInvalidActionTypeDebug() {
        runner.disableControllerService(logHandler);
        runner.setProperty(logHandler, AlertHandler.ENFORCE_ACTION_TYPE, "LOG");
        runner.setProperty(logHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "IGNORE");
        runner.enableControllerService(logHandler);

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        attributes.put("logLevel", "FAKE");

        final String expectedMessage = "--------------------------------------------------\n" +
                "Log Message: Rules Action Triggered Log.\n" +
                "Log Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes);
        try {
            logHandler.execute(action, metrics);
        } catch (UnsupportedOperationException ex) {
            fail();
        }

        final String debugMessage = mockComponentLog.getDebugMessage();
        assertTrue(StringUtils.isNotEmpty(debugMessage));
        TestCase.assertEquals("This Action Handler does not support actions with the provided type: FAKE",debugMessage);
    }

    @Test
    public void testValidActionType() {
        runner.disableControllerService(logHandler);
        runner.setProperty(logHandler, AlertHandler.ENFORCE_ACTION_TYPE, "LOG");
        runner.enableControllerService(logHandler);

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        attributes.put("logLevel", "FAKE");

        final String expectedMessage = "--------------------------------------------------\n" +
                "Log Message: Rules Action Triggered Log.\n" +
                "Log Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        try {
            logHandler.execute(action, metrics);
            assertTrue(true);
        } catch (UnsupportedOperationException ex) {
            fail();
        }
    }

    private static class MockLogHandler extends LogHandler {
        private ComponentLog testLogger;

        public MockLogHandler(ComponentLog testLogger) {
            this.testLogger = testLogger;
        }

        @Override
        protected ComponentLog getLogger() {
            return testLogger;
        }
    }


}
