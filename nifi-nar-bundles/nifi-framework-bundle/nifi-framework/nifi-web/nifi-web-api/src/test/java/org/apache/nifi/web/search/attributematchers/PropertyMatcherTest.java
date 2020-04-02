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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ProcessorNode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class PropertyMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessorNode component;

    @Before
    public void setUp() {
        super.setUp();
    }

    @Test
    public void testMatchingAndNotFiltered() {
        // given
        final PropertyMatcher testSubject = new PropertyMatcher();
        givenProperties(false);
        givenSearchTerm("lorem");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Property name: loremName", //
                "Property value: loremName - loremValue", //
                "Property description: loremDescription");
    }

    @Test
    public void testMatchingAndNotFilteredButSensitive() {
        // given
        final PropertyMatcher testSubject = new PropertyMatcher();
        givenProperties(true);
        givenSearchTerm("lorem");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Property name: loremName", //
                "Property description: loremDescription");
    }

    @Test
    public void testMatchingAndFiltered() {
        // given
        final PropertyMatcher testSubject = new PropertyMatcher();
        givenProperties(false);
        givenSearchTerm("lorem");
        givenFilter("properties", "exclude");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testMatchingAndFilteredWithIncorrectValue() {
        // given
        final PropertyMatcher testSubject = new PropertyMatcher();
        givenProperties(false);
        givenSearchTerm("lorem");
        givenFilter("properties", "foobar");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Property name: loremName", //
                "Property value: loremName - loremValue", //
                "Property description: loremDescription");
    }

    private void givenProperties(final boolean isSensitive) {
        final Map<PropertyDescriptor, String> result = new HashMap<>();
        final PropertyDescriptor descriptor = new PropertyDescriptor.Builder() //
                .name("loremName") //
                .description("loremDescription") //
                .sensitive(isSensitive) //
                .build();

        result.put(descriptor, "loremValue");
        Mockito.when(component.getRawPropertyValues()).thenReturn(result);
    }
}