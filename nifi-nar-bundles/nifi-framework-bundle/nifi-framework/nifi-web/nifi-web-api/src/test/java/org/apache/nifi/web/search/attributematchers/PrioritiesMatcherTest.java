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
package org.apache.nifi.web.search.attributematchers;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class PrioritiesMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private Connection component;

    @Mock
    private FlowFileQueue flowFileQueue;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(component.getFlowFileQueue()).thenReturn(flowFileQueue);
        Mockito.when(flowFileQueue.getPriorities()).thenReturn(givenPriorizers());
    }

    @Test
    public void testMatching() {
        // given
        final PrioritiesMatcher testSubject = new PrioritiesMatcher();
        givenSearchTerm("FlowFilePrioritize");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf(
                "Prioritizer: org.apache.nifi.web.search.attributematchers.PrioritiesMatcherTest$FlowFilePrioritizerOne",
                "Prioritizer: org.apache.nifi.web.search.attributematchers.PrioritiesMatcherTest$FlowFilePrioritizerTwo");
    }

    private List<FlowFilePrioritizer> givenPriorizers() {
        final List<FlowFilePrioritizer> result = new ArrayList<>();
        result.add(new FlowFilePrioritizerOne());
        result.add(new FlowFilePrioritizerTwo());
        return result;
    }

    private static class FlowFilePrioritizerOne implements FlowFilePrioritizer {
        @Override
        public int compare(FlowFile o1, FlowFile o2) {
            return 0;
        }
    }

    private static class FlowFilePrioritizerTwo implements FlowFilePrioritizer {
        @Override
        public int compare(FlowFile o1, FlowFile o2) {
            return 0;
        }
    }
}