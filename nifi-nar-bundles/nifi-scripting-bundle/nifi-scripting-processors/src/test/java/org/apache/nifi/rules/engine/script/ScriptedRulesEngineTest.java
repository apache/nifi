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

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


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
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        final MockRecordWriter writer = new MockRecordWriter(null, false); // No header, don"t quote values
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        final MockScriptedRulesEngine rulesEngine = new MockScriptedRulesEngine();
        runner.addControllerService("rulesEngine", rulesEngine);
        runner.setProperty(rulesEngine, "Script Engine", "Groovy");
        runner.setProperty(rulesEngine, ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/groovy/test_rules_engine.groovy");
        runner.setProperty(rulesEngine, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(rulesEngine, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(rulesEngine);

        return rulesEngine;
    }

    public static class MockScriptedRulesEngine extends ScriptedRulesEngine implements AccessibleScriptingComponentHelper {

        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}