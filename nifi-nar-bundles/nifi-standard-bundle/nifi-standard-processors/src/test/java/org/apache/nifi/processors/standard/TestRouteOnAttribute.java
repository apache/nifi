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
package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.VariableRegistryUtils;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestRouteOnAttribute {

    private VariableRegistry variableRegistry = VariableRegistryUtils.createSystemVariableRegistry();

    @Test
    public void testInvalidOnMisconfiguredProperty() {
        final RouteOnAttribute proc = new RouteOnAttribute();
        final MockProcessContext ctx = new MockProcessContext(proc, new MockStateManager(proc), variableRegistry);
        final ValidationResult validationResult = ctx.setProperty("RouteA", "${a:equals('b')"); // Missing closing brace
        assertFalse(validationResult.isValid());
    }

    @Test
    public void testInvalidOnNonBooleanProperty() {
        final RouteOnAttribute proc = new RouteOnAttribute();
        final MockProcessContext ctx = new MockProcessContext(proc, new MockStateManager(proc), variableRegistry);
        final ValidationResult validationResult = ctx.setProperty("RouteA", "${a:length()"); // Should be boolean
        assertFalse(validationResult.isValid());
    }

    @Test
    public void testSimpleEquals() {
        final TestRunner runner = TestRunners.newTestRunner(new RouteOnAttribute());
        runner.setProperty("RouteA", "${a:equals('b')}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "b");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(new Relationship.Builder().name("RouteA").build(), 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("RouteA");
        flowFiles.get(0).assertAttributeEquals("a", "b");
        flowFiles.get(0).assertAttributeEquals(RouteOnAttribute.ROUTE_ATTRIBUTE_KEY, "RouteA");
    }

    @Test
    public void testMatchAll() {
        final TestRunner runner = TestRunners.newTestRunner(new RouteOnAttribute());
        runner.setProperty(RouteOnAttribute.ROUTE_STRATEGY, RouteOnAttribute.ROUTE_ALL_MATCH.getValue());
        runner.setProperty("RouteA", "${a:equals('b')}");
        runner.setProperty("RouteB", "${b:equals('a')}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "b");
        attributes.put("b", "a");
        runner.enqueue(new byte[0], attributes);

        attributes.put("b", "b");
        runner.enqueue(new byte[0], attributes);

        attributes.put("a", "a");
        attributes.put("b", "b");
        runner.enqueue(new byte[0], attributes);

        runner.enqueue(new byte[0]);

        runner.run(4);

        final List<MockFlowFile> match = runner.getFlowFilesForRelationship(RouteOnAttribute.REL_MATCH);
        final List<MockFlowFile> noMatch = runner.getFlowFilesForRelationship(RouteOnAttribute.REL_NO_MATCH);

        assertEquals(1, match.size());
        assertEquals(3, noMatch.size());

        for (final MockFlowFile ff : noMatch) {
            ff.assertAttributeEquals(RouteOnAttribute.ROUTE_ATTRIBUTE_KEY, "unmatched");
        }

        final Map<String, String> matchedAttrs = match.iterator().next().getAttributes();
        assertEquals("b", matchedAttrs.get("a"));
        assertEquals("a", matchedAttrs.get("b"));
        assertEquals("matched", matchedAttrs.get(RouteOnAttribute.ROUTE_ATTRIBUTE_KEY));
    }

    @Test
    public void testMatchAny() {
        final TestRunner runner = TestRunners.newTestRunner(new RouteOnAttribute());
        runner.setThreadCount(4);
        runner.setProperty(RouteOnAttribute.ROUTE_STRATEGY, RouteOnAttribute.ROUTE_ANY_MATCHES.getValue());
        runner.setProperty("RouteA", "${a:equals('b')}");
        runner.setProperty("RouteB", "${b:equals('a')}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "b");
        attributes.put("b", "a");
        runner.enqueue(new byte[0], attributes);

        attributes.put("b", "b");
        runner.enqueue(new byte[0], attributes);

        attributes.put("a", "a");
        attributes.put("b", "b");
        runner.enqueue(new byte[0], attributes);

        runner.enqueue(new byte[0]);

        runner.run(4);

        final List<MockFlowFile> match = runner.getFlowFilesForRelationship(RouteOnAttribute.REL_MATCH);
        final List<MockFlowFile> noMatch = runner.getFlowFilesForRelationship(RouteOnAttribute.REL_NO_MATCH);

        assertEquals(2, match.size());
        assertEquals(2, noMatch.size());

        // Get attributes for both matching FlowFiles
        final Iterator<MockFlowFile> itr = match.iterator();
        final Map<String, String> attrs1 = itr.next().getAttributes();
        final Map<String, String> attrs2 = itr.next().getAttributes();

        // Both matches should map a -> b
        assertEquals("b", attrs1.get("a"));
        assertEquals("b", attrs1.get("a"));

        // One of the flowfiles should map  b -> a, the other b -> b, but we
        // can't know which order they'll come out in, since we're running 4 threads. So either way is acceptable.
        if ("a".equals(attrs1.get("b"))) {
            assertEquals("b", attrs2.get("b"));
        } else {
            assertEquals("b", attrs1.get("b"));
            assertEquals("a", attrs2.get("b"));
        }
        runner.clearTransferState();
    }

}
