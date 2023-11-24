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
package org.apache.nifi.reporting.script;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.processors.script.ScriptRunner;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockEventAccess;
import org.apache.nifi.util.MockReportingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.script.ScriptEngine;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ScriptedReportingTask.
 */
@ExtendWith(MockitoExtension.class)
class ScriptedReportingTaskTest {
    private static final String SCRIPT_ENGINE = "Script Engine";
    private static final PropertyDescriptor SCRIPT_ENGINE_PROPERTY_DESCRIPTOR = new PropertyDescriptor.Builder().name(SCRIPT_ENGINE).build();
    private static final String GROOVY = "Groovy";
    @TempDir
    private Path targetPath;
    @Mock
    private ReportingInitializationContext initContext;
    private MockScriptedReportingTask task;
    private Map<PropertyDescriptor, String> properties;
    private ConfigurationContext configurationContext;
    private MockReportingContext reportingContext;

    @BeforeEach
    public void setUp(@Mock ComponentLog logger) {
        task = new MockScriptedReportingTask();
        properties = new HashMap<>();
        configurationContext = new MockConfigurationContext(properties, null, null);
        reportingContext = new MockReportingContext(new LinkedHashMap<>(), null);
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(initContext.getLogger()).thenReturn(logger);
    }

    @Test
    void testProvenanceGroovyScript() throws Exception {
        properties.put(SCRIPT_ENGINE_PROPERTY_DESCRIPTOR, GROOVY);
        Files.copy(Paths.get("src/test/resources/groovy/test_log_provenance_events.groovy"), targetPath, StandardCopyOption.REPLACE_EXISTING);
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());
        reportingContext.setProperty(SCRIPT_ENGINE, GROOVY);
        reportingContext.setProperty(ScriptingComponentUtils.SCRIPT_FILE.getName(), targetPath.toString());

        final MockEventAccess eventAccess = reportingContext.getEventAccess();
        for (long index = 1; index < 4; index++) {
            final ProvenanceEventRecord event = mock(ProvenanceEventRecord.class);
            doReturn(index).when(event).getEventId();
            if(index == 1) {
                doReturn("1234").when(event).getComponentId();
                Map<String, String> map = new LinkedHashMap<>(1);
                map.put("abc", "xyz");
                doReturn(map).when(event).getAttributes();
            }
            eventAccess.addProvenanceEvent(event);
        }

        run();

        // This script should return a variable x with the number of events and a variable e with the first event
        ScriptEngine se = task.getScriptRunner().getScriptEngine();
        assertEquals(3, se.get("x"));
        ProvenanceEventRecord per = (ProvenanceEventRecord)se.get("e");
        assertEquals("1234", per.getComponentId());
        assertEquals("xyz",per.getAttributes().get("abc"));
    }

    @Test
    void testVMEventsGroovyScript() throws Exception {
        properties.put(SCRIPT_ENGINE_PROPERTY_DESCRIPTOR, GROOVY);
        Files.copy(Paths.get("src/test/resources/groovy/test_log_vm_stats.groovy"), targetPath, StandardCopyOption.REPLACE_EXISTING);
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());
        reportingContext.setProperty(SCRIPT_ENGINE, GROOVY);
        reportingContext.setProperty(ScriptingComponentUtils.SCRIPT_FILE.getName(), targetPath.toString());

        run();

        // This script should store a variable called x with a map of stats to values
        ScriptEngine se = task.getScriptRunner().getScriptEngine();
        @SuppressWarnings("unchecked")
        final Map<String, Long> x = (Map<String, Long>)se.get("x");
        assertTrue(x.get("uptime") >= 0);
    }

    private void run() throws Exception {
        task.initialize(initContext);
        task.getSupportedPropertyDescriptors();
        task.setup(configurationContext);
        task.onTrigger(reportingContext);
    }

    public static class MockScriptedReportingTask extends ScriptedReportingTask implements AccessibleScriptingComponentHelper {
        public ScriptRunner getScriptRunner() {
            return getScriptingComponentHelper().scriptRunnerQ.poll();
        }

        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}
