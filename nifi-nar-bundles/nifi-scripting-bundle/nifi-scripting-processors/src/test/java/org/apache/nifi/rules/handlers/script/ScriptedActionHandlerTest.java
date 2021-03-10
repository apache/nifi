/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.rules.handlers.script;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinFactory;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.rules.Action;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockBulletinRepository;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ScriptedActionHandlerTest {

    private TestRunner runner;
    private ReportingContext reportingContext;
    private MockScriptedActionHandler actionHandler;
    private MockScriptedBulletinRepository mockScriptedBulletinRepository;

    private Map<String, Object> facts = new HashMap<>();
    private Map<String, String> attrs = new HashMap<>();

    @Before
    public void setup() {
        facts.put("predictedQueuedCount", 60);
        facts.put("predictedTimeToBytesBackpressureMillis", 299999);
        attrs.put("level", "DEBUG");
        attrs.put("message", "Time to backpressure < 5 mins");
    }

    @Test
    public void testActions() throws InitializationException {
        actionHandler = initTask("src/test/resources/groovy/test_action_handler.groovy");
        List<Action> actions = Arrays.asList(new Action("LOG", attrs), new Action("ALERT", attrs));
        actions.forEach((action) -> actionHandler.execute(action, facts));
        // Verify a fact was added (not the intended operation of ActionHandler, but testable)
        assertEquals(42, facts.get("testFact"));
    }

    @Test
    public void testActionHandlerNotPropertyContextActionHandler() throws InitializationException {
        actionHandler = initTask("src/test/resources/groovy/test_action_handler.groovy");
        mockScriptedBulletinRepository = new MockScriptedBulletinRepository();
        reportingContext = mock(ReportingContext.class);
        when(reportingContext.getBulletinRepository()).thenReturn(mockScriptedBulletinRepository);
        when(reportingContext.createBulletin(anyString(), Mockito.any(Severity.class), anyString()))
                .thenAnswer(invocation -> BulletinFactory.createBulletin(invocation.getArgument(0), invocation.getArgument(1).toString(), invocation.getArgument(2)));
        List<Action> actions = Arrays.asList(new Action("LOG", attrs), new Action("ALERT", attrs));
        actions.forEach(action -> actionHandler.execute(reportingContext, action, facts));

        // Verify instead of a bulletin being added, a fact was added (not the intended operation of ActionHandler, but testable)
        assertTrue(mockScriptedBulletinRepository.bulletinList.isEmpty());
        assertEquals(42, facts.get("testFact"));
    }

    @Test
    public void testPropertyContextActionHandler() throws InitializationException {
        actionHandler = initTask("src/test/resources/groovy/test_propertycontext_action_handler.groovy");
        mockScriptedBulletinRepository = new MockScriptedBulletinRepository();
        reportingContext = mock(ReportingContext.class);
        when(reportingContext.getBulletinRepository()).thenReturn(mockScriptedBulletinRepository);
        when(reportingContext.createBulletin(anyString(), Mockito.any(Severity.class), anyString()))
                .thenAnswer(invocation -> BulletinFactory.createBulletin(invocation.getArgument(0), invocation.getArgument(1).toString(), invocation.getArgument(2)));
        List<Action> actions = Arrays.asList(new Action("LOG", attrs), new Action("ALERT", attrs));
        actions.forEach(action -> actionHandler.execute(reportingContext, action, facts));

        // Verify instead of a bulletin being added, a fact was added (not the intended operation of ActionHandler, but testable)
        List<Bulletin> bulletinList = mockScriptedBulletinRepository.bulletinList;
        assertEquals(2, bulletinList.size());
    }

    @Test
    public void testValidService() throws Exception {
        setupTestRunner();
        runner.assertValid(actionHandler);
        assertThat(actionHandler, instanceOf(ScriptedActionHandler.class));
    }

    @Test
    public void testAlertWithBulletinLevel() throws Exception {
        setupTestRunner();
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();

        final String category = "Rules Alert";
        final String message = "This should be sent as an alert!";
        final String severity = "INFO";
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
        actionHandler.execute(reportingContext, action, metrics);
        BulletinRepository bulletinRepository = reportingContext.getBulletinRepository();
        List<Bulletin> bulletins = bulletinRepository.findBulletinsForController();
        assertFalse(bulletins.isEmpty());
        Bulletin bulletin = bulletins.get(0);
        assertEquals(bulletin.getCategory(), category);
        assertEquals(bulletin.getMessage(), expectedOutput);
        assertEquals(bulletin.getLevel(), severity);
    }

    private static class MockScriptedBulletinRepository extends MockBulletinRepository {

        List<Bulletin> bulletinList;

        MockScriptedBulletinRepository() {
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

    private void setupTestRunner() throws Exception {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        MockScriptedActionHandler handler = initTask("src/test/resources/groovy/test_propertycontext_action_handler.groovy");
        mockScriptedBulletinRepository = new MockScriptedBulletinRepository();
        Map<String, String> properties = new HashMap<>();
        properties.put(handler.getScriptingComponentHelper().SCRIPT_ENGINE.getName(), "Groovy");
        properties.put(ScriptingComponentUtils.SCRIPT_FILE.getName(), "src/test/resources/groovy/test_propertycontext_action_handler.groovy");
        runner.addControllerService("MockAlertHandler", handler, properties);
        runner.enableControllerService(handler);
        actionHandler = (MockScriptedActionHandler) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("MockAlertHandler");
        reportingContext = mock(ReportingContext.class);
        when(reportingContext.getBulletinRepository()).thenReturn(mockScriptedBulletinRepository);
        when(reportingContext.createBulletin(anyString(), Mockito.any(Severity.class), anyString()))
                .thenAnswer(invocation -> BulletinFactory.createBulletin(invocation.getArgument(0), invocation.getArgument(1).toString(), invocation.getArgument(2)));
    }

    private MockScriptedActionHandler initTask(String scriptFile) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        final MockRecordWriter writer = new MockRecordWriter(null, false); // No header, don"t quote values
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        final MockScriptedActionHandler actionHandler = new MockScriptedActionHandler();
        runner.addControllerService("actionHandler", actionHandler);
        runner.setProperty(actionHandler, "Script Engine", "Groovy");
        runner.setProperty(actionHandler, ScriptingComponentUtils.SCRIPT_FILE, scriptFile);
        runner.setProperty(actionHandler, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(actionHandler, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(actionHandler);

        return actionHandler;
    }

    public static class MockScriptedActionHandler extends ScriptedActionHandler implements AccessibleScriptingComponentHelper {

        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}