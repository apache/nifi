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

import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ParameterMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private Parameter parameter;

    @Mock
    private ParameterDescriptor descriptor;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(parameter.getDescriptor()).thenReturn(descriptor);
        Mockito.when(parameter.getValue()).thenReturn("LoremValue");
        Mockito.when(descriptor.getName()).thenReturn("LoremName");
        Mockito.when(descriptor.getDescription()).thenReturn("LoremDescription");
    }

    @Test
    public void testMatchingWhenNotSensitive() {
        // given
        final ParameterMatcher testSubject = new ParameterMatcher();
        givenValueIsNotSensitive();

        // when
        testSubject.match(parameter, searchQuery, matches);

        // then
        thenMatchConsistsOf("Name: LoremName", "Value: LoremValue", "Description: LoremDescription");
    }

    @Test
    public void testMatchingWhenSensitive() {
        // given
        final ParameterMatcher testSubject = new ParameterMatcher();
        givenValueIsSensitive();

        // when
        testSubject.match(parameter, searchQuery, matches);

        // then
        thenMatchConsistsOf("Name: LoremName", "Description: LoremDescription");
    }

    private void givenValueIsSensitive() {
        Mockito.when(descriptor.isSensitive()).thenReturn(true);
    }

    private void givenValueIsNotSensitive() {
        Mockito.when(descriptor.isSensitive()).thenReturn(false);
    }
}