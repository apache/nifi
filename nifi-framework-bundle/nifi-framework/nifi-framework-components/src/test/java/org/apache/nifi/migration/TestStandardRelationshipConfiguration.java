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

package org.apache.nifi.migration;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.processor.Relationship;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestStandardRelationshipConfiguration {

    private final String SUCCESS = "success";
    private final String UNSUCCESSFUL = "unsuccessful";
    private final String PARTIAL_SUCCESS = "partial-success";
    private final Relationship REL_SUCCESS = createRelationship(SUCCESS);
    private final Relationship REL_UNSUCCESSFUL = createRelationship(UNSUCCESSFUL);
    private final Relationship REL_PARTIAL_SUCCESS = createRelationship(PARTIAL_SUCCESS);

    private ProcessorNode processorNode;
    private final Set<Relationship> autoTerminatedRelationships = new HashSet<>();
    private final Set<Connection> connections = new HashSet<>();
    private final Set<String> retriedRelationships = new HashSet<>();

    @BeforeEach
    public void setup() {
        processorNode = mock(ProcessorNode.class);

        when(processorNode.getAutoTerminatedRelationships()).thenReturn(autoTerminatedRelationships);
        doAnswer(invocation -> {
            this.autoTerminatedRelationships.clear();
            final Set<Relationship> set = (Set<Relationship>) invocation.getArgument(0, Set.class);
            this.autoTerminatedRelationships.addAll(set);
            return null;
        }).when(processorNode).setAutoTerminatedRelationships(anySet());

        when(processorNode.getConnections()).thenReturn(connections);

        when(processorNode.getRetriedRelationships()).thenReturn(retriedRelationships);
        doAnswer(invocation -> {
            this.retriedRelationships.clear();
            final Set<String> retried = invocation.getArgument(0, Set.class);
            this.retriedRelationships.addAll(retried);
            return null;
        }).when(processorNode).setRetriedRelationships(anySet());

        when(processorNode.getRelationship(anyString())).thenAnswer(invocation -> createRelationship(invocation.getArgument(0, String.class)));

        when(processorNode.getConnections(any(Relationship.class))).thenAnswer(invocation -> {
            final Relationship relationship = invocation.getArgument(0, Relationship.class);
            return connections.stream()
                .filter(conn -> conn.getRelationships().contains(relationship))
                .collect(Collectors.toSet());
        });
    }

    private Relationship createRelationship(final String name) {
        return new Relationship.Builder().name(name).build();
    }

    private void addConnection(final String relationshipName) {
        addConnection(Collections.singleton(relationshipName));
    }

    private void addConnection(final Collection<String> relationshipNames) {
        final Connection connection = mock(Connection.class);
        connections.add(connection);

        final Set<Relationship> relationships = new HashSet<>();
        relationshipNames.stream()
            .map(this::createRelationship)
            .forEach(relationships::add);

        when(connection.getRelationships()).thenReturn(relationships);

        doAnswer(invocation -> {
            relationships.clear();
            final Collection<Relationship> set = invocation.getArgument(0, Collection.class);
            relationships.addAll(set);
            return null;
        }).when(connection).setRelationships(anyCollection());
    }

    @Test
    public void testRenameRelationship() {
        addConnection(SUCCESS);
        addConnection(SUCCESS);
        addConnection(PARTIAL_SUCCESS);
        autoTerminatedRelationships.add(REL_PARTIAL_SUCCESS);
        retriedRelationships.add(SUCCESS);

        final StandardRelationshipConfiguration config = new StandardRelationshipConfiguration(processorNode);

        assertTrue(config.renameRelationship(SUCCESS, UNSUCCESSFUL));

        assertFalse(autoTerminatedRelationships.contains(REL_SUCCESS));
        assertFalse(autoTerminatedRelationships.contains(REL_UNSUCCESSFUL));

        final Set<Relationship> unsuccessfulSet = Collections.singleton(REL_UNSUCCESSFUL);
        final Set<Relationship> partialSuccessfulSet = Collections.singleton(REL_PARTIAL_SUCCESS);
        int unsuccessfulCount = 0;
        int partialSuccessCount = 0;
        for (final Connection connection : connections) {
            final Collection<Relationship> relationships = connection.getRelationships();
            if (unsuccessfulSet.equals(relationships)) {
                unsuccessfulCount++;
            } else if (partialSuccessfulSet.equals(relationships)) {
                partialSuccessCount++;
            } else {
                fail("Expected the relationships to be 'unsuccessful' or 'partial-success' but was " + relationships);
            }
        }

        assertEquals(2, unsuccessfulCount);
        assertEquals(1, partialSuccessCount);

        assertEquals(Collections.singleton(REL_PARTIAL_SUCCESS), autoTerminatedRelationships);
        assertEquals(Collections.singleton(UNSUCCESSFUL), retriedRelationships);
    }

    @Test
    public void testSplitWithConflict() {
        addConnection(SUCCESS);
        addConnection(SUCCESS);
        addConnection(PARTIAL_SUCCESS);
        retriedRelationships.add(PARTIAL_SUCCESS);

        final StandardRelationshipConfiguration config = new StandardRelationshipConfiguration(processorNode);
        assertThrows(IllegalStateException.class, () -> config.splitRelationship(SUCCESS, SUCCESS, PARTIAL_SUCCESS));
    }

    @Test
    public void testSplitToSamePlusAnother() {
        addConnection(SUCCESS);
        addConnection(SUCCESS);
        retriedRelationships.add(SUCCESS);

        final StandardRelationshipConfiguration config = new StandardRelationshipConfiguration(processorNode);
        assertTrue(config.splitRelationship(SUCCESS, SUCCESS, PARTIAL_SUCCESS));

        for (final Connection connection : connections) {
            final Collection<Relationship> rels = connection.getRelationships();
            assertEquals(Set.of(REL_SUCCESS, REL_PARTIAL_SUCCESS), rels);
        }

        assertEquals(Set.of(SUCCESS, PARTIAL_SUCCESS), retriedRelationships);
    }

    @Test
    public void testSplitToTwoOthers() {
        addConnection(SUCCESS);
        addConnection(SUCCESS);
        retriedRelationships.add(SUCCESS);

        final StandardRelationshipConfiguration config = new StandardRelationshipConfiguration(processorNode);
        assertTrue(config.splitRelationship(SUCCESS, UNSUCCESSFUL, PARTIAL_SUCCESS));

        for (final Connection connection : connections) {
            final Collection<Relationship> rels = connection.getRelationships();
            assertEquals(Set.of(REL_UNSUCCESSFUL, REL_PARTIAL_SUCCESS), rels);
        }

        assertEquals(Set.of(UNSUCCESSFUL, PARTIAL_SUCCESS), retriedRelationships);
    }
}
