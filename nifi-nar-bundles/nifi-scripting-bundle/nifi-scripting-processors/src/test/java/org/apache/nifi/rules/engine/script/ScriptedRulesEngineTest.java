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
package org.apache.nifi.rules.engine.script;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ScriptedRulesEngineTest {

    private Map<String, Object> facts = new HashMap<>();

    @Before
    public void setup() {
        facts.put("predictedQueuedCount", 60);
        facts.put("predictedTimeToBytesBackpressureMillis", 299999);
    }

    @Test
    public void testRules() throws IOException, InitializationException {
        ScriptedRulesEngine task = initTask();
        List<Action> actions = task.fireRules(facts);
        assertEquals(2, actions.size());
        assertEquals("LOG", actions.get(0).getType());
        assertEquals("DEBUG", actions.get(0).getAttributes().get("level"));
        assertEquals("ALERT", actions.get(1).getType());
        assertEquals("Time to backpressure < 5 mins", actions.get(1).getAttributes().get("message"));
    }

    private MockScriptedRulesEngine initTask() throws InitializationException {

        final MockScriptedRulesEngine rulesEngine = new MockScriptedRulesEngine();
        ConfigurationContext context = mock(ConfigurationContext.class);
        StateManager stateManager = new MockStateManager(rulesEngine);

        final ComponentLog logger = mock(ComponentLog.class);
        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(rulesEngine, UUID.randomUUID().toString(), logger, stateManager);
        rulesEngine.initialize(initContext);

        // Call something that sets up the ScriptingComponentHelper, so we can mock it
        rulesEngine.getSupportedPropertyDescriptors();

        when(context.getProperty(rulesEngine.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue("Groovy"));
        when(context.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue("src/test/resources/groovy/test_rules_engine.groovy"));
        when(context.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null));
        when(context.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null));
        try {
            rulesEngine.onEnabled(context);
        } catch (Exception e) {
            e.printStackTrace();
            fail("onEnabled error: " + e.getMessage());
        }
        return rulesEngine;
    }

    public static class MockScriptedRulesEngine extends ScriptedRulesEngine implements AccessibleScriptingComponentHelper {

        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}