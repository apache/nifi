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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinFactory;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.rules.Action;
import org.apache.nifi.util.MockBulletinRepository;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;

public class TestAlertHandler {

    private TestRunner runner;
    private MockComponentLog mockComponentLog;
    private ReportingContext reportingContext;
    private AlertHandler alertHandler;
    private MockAlertBulletinRepository mockAlertBulletinRepository;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        mockComponentLog = new MockComponentLog();
        AlertHandler handler = new MockAlertHandler(mockComponentLog);
        mockAlertBulletinRepository = new MockAlertBulletinRepository();
        runner.addControllerService("MockAlertHandler", handler);
        runner.enableControllerService(handler);
        alertHandler = (AlertHandler) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("MockAlertHandler");
        reportingContext = Mockito.mock(ReportingContext.class);
        Mockito.when(reportingContext.getBulletinRepository()).thenReturn(mockAlertBulletinRepository);
        Mockito.when(reportingContext.createBulletin(anyString(), Mockito.any(Severity.class), anyString()))
                .thenAnswer(invocation ->
                BulletinFactory.createBulletin(invocation.getArgument(0), invocation.getArgument(1).toString(), invocation.getArgument(2)));
    }

    @Test
    public void testValidService() {
        runner.assertValid(alertHandler);
        assertThat(alertHandler, instanceOf(AlertHandler.class));
    }

    @Test
    public void testAlertNoReportingContext() {

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        attributes.put("logLevel", "INFO");
        attributes.put("message", "This should be not sent as an alert!");
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        try {
            alertHandler.execute(action, metrics);
            fail();
        } catch (UnsupportedOperationException ex) {
            assertTrue(true);
        }
    }

    @Test
    public void testAlertWithBulletinLevel() {

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity =  "INFO";
        attributes.put("category", category);
        attributes.put("message", message);
        attributes.put("severity", severity);
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final String expectedOutput = "This should be sent as an alert!\n" +
                "Alert Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000\n";

        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        alertHandler.execute(reportingContext, action, metrics);
        BulletinRepository bulletinRepository = reportingContext.getBulletinRepository();
        List<Bulletin> bulletins = bulletinRepository.findBulletinsForController();
        assertFalse(bulletins.isEmpty());
        Bulletin bulletin = bulletins.get(0);
        assertEquals(bulletin.getCategory(), category);
        assertEquals(bulletin.getMessage(), expectedOutput);
        assertEquals(bulletin.getLevel(), severity);
    }

    @Test
    public void testAlertWithDefaultValues() {

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Triggered Alert";
        final String message = "An alert was triggered by a rules based action.";
        final String severity =  "INFO";
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final String expectedOutput = "An alert was triggered by a rules-based action.\n" +
                "Alert Facts:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000\n";

        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        alertHandler.execute(reportingContext, action, metrics);
        BulletinRepository bulletinRepository = reportingContext.getBulletinRepository();
        List<Bulletin> bulletins = bulletinRepository.findBulletinsForController();
        assertFalse(bulletins.isEmpty());
        Bulletin bulletin = bulletins.get(0);
        assertEquals(bulletin.getCategory(), category);
        assertEquals(bulletin.getMessage(), expectedOutput);
        assertEquals(bulletin.getLevel(), severity);
    }

    @Test
    public void testInvalidContext(){
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity =  "INFO";
        attributes.put("category", category);
        attributes.put("message", message);
        attributes.put("severity", severity);
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        PropertyContext fakeContext = new PropertyContext() {
            @Override
            public PropertyValue getProperty(PropertyDescriptor descriptor) {
                return null;
            }

            @Override
            public Map<String, String> getAllProperties() {
                return null;
            }
        };
        alertHandler.execute(fakeContext, action, metrics);
        final String debugMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(debugMessage));
        assertEquals(debugMessage,"Reporting context was not provided to create bulletins.");
    }

    @Test
    public void testEmptyBulletinRepository(){
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity =  "INFO";
        attributes.put("category", category);
        attributes.put("message", message);
        attributes.put("severity", severity);
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        ReportingContext fakeContext = Mockito.mock(ReportingContext.class);
        Mockito.when(reportingContext.getBulletinRepository()).thenReturn(null);
        alertHandler.execute(fakeContext, action, metrics);
        final String warnMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(warnMessage));
        assertEquals(warnMessage,"Bulletin Repository is not available which is unusual. Cannot send a bulletin.");
    }

    @Test
    public void testInvalidActionTypeException(){

        runner.disableControllerService(alertHandler);
        runner.setProperty(alertHandler, AlertHandler.ENFORCE_ACTION_TYPE, "ALERT");
        runner.setProperty(alertHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "EXCEPTION");
        runner.enableControllerService(alertHandler);
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity =  "INFO";
        attributes.put("category", category);
        attributes.put("message", message);
        attributes.put("severity", severity);
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes);
        try {
            alertHandler.execute(reportingContext, action, metrics);
            fail();
        } catch (UnsupportedOperationException ex) {
        }
    }

    @Test
    public void testInvalidActionTypeWarn(){

        runner.disableControllerService(alertHandler);
        runner.setProperty(alertHandler, AlertHandler.ENFORCE_ACTION_TYPE, "ALERT");
        runner.setProperty(alertHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "WARN");
        runner.enableControllerService(alertHandler);
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity =  "INFO";
        attributes.put("category", category);
        attributes.put("message", message);
        attributes.put("severity", severity);
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes);
        try {
            alertHandler.execute(reportingContext,action, metrics);
            assertTrue(true);
        } catch (UnsupportedOperationException ex) {
            fail();
        }
        final String warnMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(warnMessage));
        assertEquals("This Action Handler does not support actions with the provided type: FAKE",warnMessage);
    }

    @Test
    public void testInvalidActionTypeIgnore(){

        runner.disableControllerService(alertHandler);
        runner.setProperty(alertHandler, AlertHandler.ENFORCE_ACTION_TYPE, "ALERT");
        runner.setProperty(alertHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "IGNORE");
        runner.enableControllerService(alertHandler);
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity =  "INFO";
        attributes.put("category", category);
        attributes.put("message", message);
        attributes.put("severity", severity);
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes);
        try {
            alertHandler.execute(reportingContext,action, metrics);
            assertTrue(true);
        } catch (UnsupportedOperationException ex) {
            fail();
        }
        final String debugMessage = mockComponentLog.getDebugMessage();
        assertTrue(StringUtils.isNotEmpty(debugMessage));
        assertEquals("This Action Handler does not support actions with the provided type: FAKE",debugMessage);
    }

    @Test
    public void testValidActionType(){
        runner.disableControllerService(alertHandler);
        runner.setProperty(alertHandler, AlertHandler.ENFORCE_ACTION_TYPE, "ALERT, LOG, ");
        runner.enableControllerService(alertHandler);
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity =  "INFO";
        attributes.put("category", category);
        attributes.put("message", message);
        attributes.put("severity", severity);
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");

        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        try {
            alertHandler.execute(reportingContext,action, metrics);
            assertTrue(true);
        } catch (UnsupportedOperationException ex) {
            fail();
        }
    }

    private static class MockAlertHandler extends AlertHandler {

        private ComponentLog testLogger;

        public MockAlertHandler(ComponentLog testLogger) {
            this.testLogger = testLogger;
        }

        @Override
        protected ComponentLog getLogger() {
            return testLogger;
        }

    }

    private static class MockAlertBulletinRepository extends MockBulletinRepository {

        List<Bulletin> bulletinList;


        public MockAlertBulletinRepository() {
            bulletinList = new ArrayList<>();
        }

        @Override
        public void addBulletin(Bulletin bulletin) {
            bulletinList.add(bulletin);
        }

        @Override
        public List<Bulletin> findBulletinsForController() {
            return bulletinList;
        }

    }

}
