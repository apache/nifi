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

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class VariableRegistryMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ComponentVariableRegistry variableRegistry;

    @Mock
    private ProcessGroup processGroup;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(processGroup.getVariableRegistry()).thenReturn(variableRegistry);
        Mockito.when(variableRegistry.getVariableMap()).thenReturn(givenVariables());
    }

    @Test
    public void testMatchForOneVariable() {
        // given
        final VariableRegistryMatcher testSubject = new VariableRegistryMatcher();
        givenSearchTerm("ccc");

        // when
        testSubject.match(processGroup, searchQuery, matches);

        // then
        thenMatchConsistsOf("Variable Name: ccc", "Variable Value: ccc");
    }

    @Test
    public void testMatchForMultipleVariable() {
        // given
        final VariableRegistryMatcher testSubject = new VariableRegistryMatcher();
        givenSearchTerm("aaa");

        // when
        testSubject.match(processGroup, searchQuery, matches);

        // then
        thenMatchConsistsOf("Variable Name: aaa", "Variable Value: aaa");
    }

    @Test
    public void testMatchForDescription() {
        // given
        final VariableRegistryMatcher testSubject = new VariableRegistryMatcher();
        givenSearchTerm("description");

        // when
        testSubject.match(processGroup, searchQuery, matches);

        // then
        thenNoMatches();
    }

   Map<VariableDescriptor, String> givenVariables() {
        final Map<VariableDescriptor, String> result = new HashMap<>();
        result.put(new VariableDescriptor.Builder("aaa").description("description").sensitive(false).build(), "bbb");
        result.put(new VariableDescriptor.Builder("bbb").description("description").sensitive(false).build(), "aaa");
        result.put(new VariableDescriptor.Builder("ccc").description("description").sensitive(false).build(), "ccc");
        return result;
   }
}