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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.ComponentNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.when;

class BundleMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ComponentNode component;

    @Test
    void testMatch() {
        givenBundleId("nifi-lorem-nar");
        givenDefaultSearchTerm();

        BundleMatcher<ComponentNode> testSubject = new BundleMatcher<>();
        testSubject.match(component, searchQuery, matches);

        thenMatchConsistsOf("Bundle: nifi-lorem-nar");
    }

    @Test
    void testNoMatches() {
        givenBundleId("nifi-standard-nar");
        givenDefaultSearchTerm();

        BundleMatcher<ComponentNode> testSubject = new BundleMatcher<>();
        testSubject.match(component, searchQuery, matches);

        thenNoMatches();
    }

    private void givenBundleId(String id) {
        BundleCoordinate coordinate = new BundleCoordinate(null, id, null);
        when(component.getBundleCoordinate()).thenReturn(coordinate);
    }
}
