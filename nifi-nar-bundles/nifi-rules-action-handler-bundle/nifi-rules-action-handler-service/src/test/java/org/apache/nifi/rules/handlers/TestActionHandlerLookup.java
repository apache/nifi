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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestActionHandlerLookup {

    private MockPropertyActionHandler alertHandler;
    private MockPropertyActionHandler logHandler;
    private ActionHandlerLookup actionHandlerLookup;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        alertHandler = new MockPropertyActionHandler();
        logHandler = new MockPropertyActionHandler();
        actionHandlerLookup = new ActionHandlerLookup();

        runner = TestRunners.newTestRunner(TestProcessor.class);

        final String alertIdentifier = "alert-handler";
        runner.addControllerService(alertIdentifier, alertHandler);

        final String logIdentifier = "log-handler";
        runner.addControllerService(logIdentifier, logHandler);

        runner.addControllerService("action-handler-lookup", actionHandlerLookup);
        runner.setProperty(actionHandlerLookup, "ALERT", alertIdentifier);
        runner.setProperty(actionHandlerLookup, "LOG", logIdentifier);

        runner.enableControllerService(alertHandler);
        runner.enableControllerService(logHandler);
        runner.enableControllerService(actionHandlerLookup);

    }

    @Test
    public void testLookupAlert() {
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();
        attributes.put("logLevel", "INFO");
        attributes.put("message", "This should be not sent as an alert!");
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");
        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        actionHandlerLookup.execute(null, action, metrics);
        assert alertHandler.getExecuteContextCalled();
    }

    @Test
    public void testLookupLog() {
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();
        attributes.put("logLevel", "INFO");
        attributes.put("message", "This should be not sent as an alert!");
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");
        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        actionHandlerLookup.execute(null, action, metrics);
        assert logHandler.getExecuteContextCalled();
    }

    @Test(expected = ProcessException.class)
    public void testLookupInvalidActionType() {
        final Map<String, String> attributes = new HashMap<>();
        final Map<String, Object> metrics = new HashMap<>();
        attributes.put("logLevel", "INFO");
        attributes.put("message", "This should be not sent as an alert!");
        metrics.put("jvmHeap", "1000000");
        metrics.put("cpu", "90");
        final Action action = new Action();
        action.setType("FAKE");
        actionHandlerLookup.execute(null,action,metrics);
    }

    private static class MockPropertyActionHandler extends AbstractActionHandlerService  {

        Boolean executeContextCalled = false;

        @Override
        public void execute(Action action, Map<String, Object> facts) {
            execute(null,action,facts);
        }

        @Override
        public void execute(PropertyContext context, Action action, Map<String, Object> facts) {
            executeContextCalled = true;
        }

        public Boolean getExecuteContextCalled() {
            return executeContextCalled;
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Arrays.asList(ENFORCE_ACTION_TYPE, ENFORCE_ACTION_TYPE_LEVEL);
        }
    }

}
