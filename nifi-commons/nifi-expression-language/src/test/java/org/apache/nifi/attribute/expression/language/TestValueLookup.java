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
package org.apache.nifi.attribute.expression.language;

import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.registry.VariableRegistry;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class TestValueLookup {

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateCustomVariableRegistry() {

        final VariableRegistry variableRegistry = VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY;
        final ValueLookup initialLookup = new ValueLookup(variableRegistry, null);
        assertTrue(initialLookup.containsKey("PATH"));
        assertFalse(initialLookup.containsKey("fake.property.3"));
        assertFalse(initialLookup.containsKey("fake"));

        final Map<String, String> otherAttrs = new HashMap<>();
        otherAttrs.put("fake", "test");
        otherAttrs.put("fake.property.3", "test me out 3, test me out 4");
        final ValueLookup newLookup = new ValueLookup(variableRegistry, null, otherAttrs);
        assertTrue(newLookup.containsKey("PATH"));
        assertTrue(newLookup.containsKey("fake.property.3"));
        assertEquals("test me out 3, test me out 4", newLookup.get("fake.property.3"));
        assertEquals("test", newLookup.get("fake"));
        assertFalse(newLookup.containsKey("filename"));

        final FlowFile fakeFile = createFlowFile();
        final ValueLookup ffLookup = new ValueLookup(variableRegistry, fakeFile, otherAttrs);
        assertTrue(ffLookup.containsKey("filename"));
        assertEquals("test", ffLookup.get("fake"));
        assertEquals("1", ffLookup.get("flowFileId"));
        assertEquals("50", ffLookup.get("fileSize"));
        assertEquals("1000", ffLookup.get("entryDate"));
        assertEquals("10000", ffLookup.get("lineageStartDate"));
        assertEquals("fakefile.txt", ffLookup.get("filename"));

        final Map<String, String> overrides = new HashMap<>();
        overrides.put("fake", "the real deal");
        final ValueLookup overriddenLookup = new ValueLookup(variableRegistry, fakeFile, overrides, otherAttrs);
        assertTrue(overriddenLookup.containsKey("filename"));
        assertEquals("the real deal", overriddenLookup.get("fake"));
        assertEquals("1", overriddenLookup.get("flowFileId"));
        assertEquals("50", overriddenLookup.get("fileSize"));
        assertEquals("1000", overriddenLookup.get("entryDate"));
        assertEquals("10000", overriddenLookup.get("lineageStartDate"));
        assertEquals("fakefile.txt", overriddenLookup.get("filename"));
        assertEquals("original", overriddenLookup.get("override me"));

        final Map<String, String> newOverrides = new HashMap<>();
        newOverrides.put("fake", "the real deal");
        newOverrides.put("override me", "done you are now overridden");
        final ValueLookup newOverriddenLookup = new ValueLookup(variableRegistry, fakeFile, newOverrides, otherAttrs);
        assertTrue(newOverriddenLookup.containsKey("filename"));
        assertEquals("the real deal", newOverriddenLookup.get("fake"));
        assertEquals("1", newOverriddenLookup.get("flowFileId"));
        assertEquals("50", newOverriddenLookup.get("fileSize"));
        assertEquals("1000", newOverriddenLookup.get("entryDate"));
        assertEquals("10000", newOverriddenLookup.get("lineageStartDate"));
        assertEquals("fakefile.txt", newOverriddenLookup.get("filename"));
        assertEquals("done you are now overridden", newOverriddenLookup.get("override me"));
    }

    private FlowFile createFlowFile() {
        return new FlowFile() {
            @Override
            public long getId() {
                return 1;
            }

            @Override
            public long getEntryDate() {
                return 1000;
            }

            @Override
            public long getLineageStartDate() {
                return 10000;
            }

            @Override
            public Long getLastQueueDate() {
                return null;
            }

            @Override
            public boolean isPenalized() {
                return false;
            }

            @Override
            public String getAttribute(String key) {
                return getAttributes().get(key);
            }

            @Override
            public long getSize() {
                return 50;
            }

            @Override
            public long getLineageStartIndex() {
                return 0;
            }

            @Override
            public long getQueueDateIndex() {
                return 0;
            }

            @Override
            public Map<String, String> getAttributes() {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("filename", "fakefile.txt");
                attributes.put("override me", "original");
                return attributes;
            }

            @Override
            public int compareTo(FlowFile o) {
                return 0;
            }
        };
    }

}
