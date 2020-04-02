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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.processor.Relationship;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.HashSet;

public class RelationshipMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private Connectable component;

    @Test
    public void testMatching() {
        // given
        final RelationshipMatcher<Connectable> testSubject = new RelationshipMatcher<>();
        givenRelationships("incoming", "outgoing1", "outgoing2");
        givenSearchTerm("outgoing");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Relationship: outgoing1", "Relationship: outgoing2");
    }

    @Test
    public void testDoesNotMatchForDescription() {
        // given
        final RelationshipMatcher<Connectable> testSubject = new RelationshipMatcher<>();
        givenRelationships("incoming", "outgoing1", "outgoing2");
        givenSearchTerm("description");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    private void givenRelationships(final String... relationshipNames) {
        final Collection<Relationship> relationships = new HashSet<>();

        for (final String relationshipName : relationshipNames) {
            final Relationship relationship = new Relationship.Builder().name(relationshipName).description("description").build();
            relationships.add(relationship);
        }

        Mockito.when(component.getRelationships()).thenReturn(relationships);
    }
}