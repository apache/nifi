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

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.scheduling.ExecutionNode;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ExecutionMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessorNode component;

    @Test
    public void testWithKeywordWhenApplies() {
        // given
        final ExecutionMatcher testSubject = new ExecutionMatcher();
        givenExecutionModeIsPrimary();
        givenSearchTerm("primary");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Execution node: primary");
    }

    @Test
    public void testWithKeywordWhenDoesNotApplies() {
        // given
        final ExecutionMatcher testSubject = new ExecutionMatcher();
        givenExecutionModeIsNotPrimary();
        givenSearchTerm("primary");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWithoutKeywordWhenDoesNotApplies() {
        // given
        final ExecutionMatcher testSubject = new ExecutionMatcher();
        givenExecutionModeIsPrimary();
        givenSearchTerm("lorem");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    private void givenExecutionModeIsPrimary() {
        Mockito.when(component.getExecutionNode()).thenReturn(ExecutionNode.PRIMARY);
    }

    private void givenExecutionModeIsNotPrimary() {
        Mockito.when(component.getExecutionNode()).thenReturn(ExecutionNode.ALL);
    }
}