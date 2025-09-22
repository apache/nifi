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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Optional;

public class ProcessGroupMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessGroup component;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        givenDefaultSearchTerm();
        Mockito.when(component.getIdentifier()).thenReturn("LoremId");
        Mockito.when(component.getVersionedComponentId()).thenReturn(Optional.of("LoremVersionId"));
        Mockito.when(component.getName()).thenReturn("LoremName");
        Mockito.when(component.getComments()).thenReturn("LoremComment");
    }

    @Test
    public void testMatching() {
        // given
        final ProcessGroupMatcher testSubject = new ProcessGroupMatcher();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Id: LoremId", //
                "Version Control ID: LoremVersionId", //
                "Name: LoremName", //
                "Comments: LoremComment");
    }
}
