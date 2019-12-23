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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestExpressionHandler {

    private TestRunner runner;
    private MockComponentLog mockComponentLog;
    private ExpressionHandler expressionHandler;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        mockComponentLog = new MockComponentLog();
        ExpressionHandler handler = new MockExpressionHandler(mockComponentLog);
        runner.addControllerService("MockExpressionHandler", handler);
        runner.enableControllerService(handler);
        expressionHandler = (ExpressionHandler) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("MockExpressionHandler");
    }

    @Test
    public void testValidService() {
        runner.assertValid(expressionHandler);
        assertThat(expressionHandler, instanceOf(ExpressionHandler.class));
    }

    @Test
    public void testMvelExpression(){

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "Expression was executed successfully:";

        attributes.put("command","System.out.println(jvmHeap)");
        attributes.put("type","MVEL");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("EXPRESSION");
        action.setAttributes(attributes);
        expressionHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getDebugMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertTrue(logMessage.startsWith(expectedMessage));
    }

    @Test
    public void testSpelExpression(){
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "Expression was executed successfully with result:";

        attributes.put("command","#jvmHeap + ' is large'");
        attributes.put("type","SPEL");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("EXPRESSION");
        action.setAttributes(attributes);
        expressionHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getDebugMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertTrue(logMessage.startsWith(expectedMessage));
    }

    @Test
    public void testInvalidType() {
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "Error occurred when attempting to execute expression.";

        attributes.put("command","#jvmHeap + ' is large'");
        attributes.put("type","FAKE");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("EXPRESSION");
        action.setAttributes(attributes);
        expressionHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertTrue(logMessage.startsWith(expectedMessage));
    }

    @Test
    public void testNoCommandProvided() {
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "Command attribute was not provided.";
        attributes.put("type","FAKE");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("EXPRESSION");
        action.setAttributes(attributes);
        expressionHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertTrue(logMessage.startsWith(expectedMessage));
    }

    @Test
    public void testInvalidActionTypeException() {
        runner.disableControllerService(expressionHandler);
        runner.setProperty(expressionHandler, AlertHandler.ENFORCE_ACTION_TYPE, "EXPRESSION");
        runner.setProperty(expressionHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "EXCEPTION");
        runner.enableControllerService(expressionHandler);
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        attributes.put("type","FAKE");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes); try {
            expressionHandler.execute(action, metrics);
            fail();
        } catch (UnsupportedOperationException ex) {
            assertTrue(true);
        }
    }

    @Test
    public void testInvalidActionTypeWarning() {
        runner.disableControllerService(expressionHandler);
        runner.setProperty(expressionHandler, AlertHandler.ENFORCE_ACTION_TYPE, "EXPRESSION");
        runner.setProperty(expressionHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "WARN");
        runner.enableControllerService(expressionHandler);
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        attributes.put("type","FAKE");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes); try {
            expressionHandler.execute(action, metrics);
        } catch (UnsupportedOperationException ex) {
            fail();
        }

        final String warnMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(warnMessage));
        assertEquals("This Action Handler does not support actions with the provided type: FAKE",warnMessage);

    }

    @Test
    public void testInvalidActionTypeIgnore() {
        runner.disableControllerService(expressionHandler);
        runner.setProperty(expressionHandler, AlertHandler.ENFORCE_ACTION_TYPE, "EXPRESSION");
        runner.setProperty(expressionHandler, AlertHandler.ENFORCE_ACTION_TYPE_LEVEL, "IGNORE");
        runner.enableControllerService(expressionHandler);
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        attributes.put("type","FAKE");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("FAKE");
        action.setAttributes(attributes); try {
            expressionHandler.execute(action, metrics);
        } catch (UnsupportedOperationException ex) {
            fail();
        }

        final String debugMessage = mockComponentLog.getDebugMessage();
        assertTrue(StringUtils.isNotEmpty(debugMessage));
        assertEquals("This Action Handler does not support actions with the provided type: FAKE",debugMessage);

    }
    @Test
    public void testValidActionType() {
        runner.disableControllerService(expressionHandler);
        runner.setProperty(expressionHandler, AlertHandler.ENFORCE_ACTION_TYPE, "EXPRESSION");
        runner.enableControllerService(expressionHandler);
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        attributes.put("type","FAKE");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        final Action action = new Action();
        action.setType("EXPRESSION");
        action.setAttributes(attributes);
        try {
            expressionHandler.execute(action, metrics);
            assertTrue(true);
        } catch (UnsupportedOperationException ex) {
            fail();
        }
    }


    private static class MockExpressionHandler extends ExpressionHandler{
        private ComponentLog testLogger;

        public MockExpressionHandler(ComponentLog testLogger) {
            this.testLogger = testLogger;
        }

        @Override
        protected ComponentLog getLogger() {
            return testLogger;
        }
    }
}
