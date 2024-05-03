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
package org.apache.nifi.prioritizer;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("EqualsWithItself")
public class PriorityAttributePrioritizerTest {

    private final TestRunner testRunner = TestRunners.newTestRunner(NoOpProcessor.class);
    private final FlowFilePrioritizer prioritizer = new PriorityAttributePrioritizer();

    @Test
    public void testPrioritizer() {
        final FlowFile ffWithoutPriority = testRunner.enqueue("data");
        final FlowFile ffWithPriority1 = enqueueWithPriority("1");
        final FlowFile ffWithPriority2 = enqueueWithPriority("2");
        final FlowFile ffWithPriorityNegative1 = enqueueWithPriority("-1");
        final FlowFile ffWithPriorityA = enqueueWithPriority("A");
        final FlowFile ffWithPriorityB = enqueueWithPriority("B");
        final FlowFile ffWithLongPriority = enqueueWithPriority("5432123456789");
        final FlowFile ffWithNegativeLongPriority = enqueueWithPriority("-5432123456789");

        assertEquals(0, prioritizer.compare(null, null));
        assertEquals(-1, prioritizer.compare(ffWithoutPriority, null));
        assertEquals(1, prioritizer.compare(null, ffWithoutPriority));

        assertEquals(0, prioritizer.compare(ffWithoutPriority, ffWithoutPriority));
        assertEquals(-1, prioritizer.compare(ffWithPriority1, ffWithoutPriority));
        assertEquals(1, prioritizer.compare(ffWithoutPriority, ffWithPriority1));

        assertEquals(0, prioritizer.compare(ffWithPriority1, ffWithPriority1));
        assertEquals(-1, prioritizer.compare(ffWithPriority1, ffWithPriority2));
        assertEquals(1, prioritizer.compare(ffWithPriority2, ffWithPriority1));
        assertEquals(-1, prioritizer.compare(ffWithPriorityNegative1, ffWithPriority1));
        assertEquals(1, prioritizer.compare(ffWithPriority1, ffWithPriorityNegative1));

        assertEquals(-1, prioritizer.compare(ffWithPriority1, ffWithPriorityA));
        assertEquals(1, prioritizer.compare(ffWithPriorityA, ffWithPriority1));

        assertEquals(0, prioritizer.compare(ffWithPriorityA, ffWithPriorityA));
        assertEquals(-1, prioritizer.compare(ffWithPriorityA, ffWithPriorityB));
        assertEquals(1, prioritizer.compare(ffWithPriorityB, ffWithPriorityA));

        assertEquals(1, prioritizer.compare(ffWithLongPriority, ffWithPriority1));
        assertEquals(-1, prioritizer.compare(ffWithPriority1, ffWithLongPriority));
        assertEquals(-1, prioritizer.compare(ffWithNegativeLongPriority, ffWithPriority1));
        assertEquals(1, prioritizer.compare(ffWithPriority1, ffWithNegativeLongPriority));
    }

    private MockFlowFile enqueueWithPriority(String priority) {
        return testRunner.enqueue("data", Map.of(CoreAttributes.PRIORITY.key(), priority));
    }
}
