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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StandardRelationshipConfiguration implements RelationshipConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(StandardRelationshipConfiguration.class);
    private final Set<String> relationships;
    private final ProcessorNode processor;

    public StandardRelationshipConfiguration(final ProcessorNode processor) {
        this.processor = processor;

        final Set<String> usedRelationships = new HashSet<>();
        processor.getAutoTerminatedRelationships().stream()
            .map(Relationship::getName)
            .forEach(usedRelationships::add);

        usedRelationships.addAll(processor.getRetriedRelationships());
        for (final Connection connection : processor.getConnections()) {
            connection.getRelationships().stream()
                .map(Relationship::getName)
                .forEach(usedRelationships::add);
        }

        relationships = usedRelationships;
    }

    @Override
    public boolean renameRelationship(final String relationshipName, final String newName) {
        if (relationshipName.equals(newName)) {
            return false;
        }

        return replaceRelationship("rename", relationshipName, newName);
    }

    @Override
    public boolean splitRelationship(final String relationshipName, final String firstRelationshipName, final String... additionalRelationshipNames) {
        return replaceRelationship("split", relationshipName, firstRelationshipName, additionalRelationshipNames);
    }

    private boolean replaceRelationship(final String task, final String relationshipName, final String firstRelationshipName, final String... additionalRelationshipNames) {
        if (!relationships.contains(relationshipName)) {
            logger.debug("Will not {} Relationship [{}] for [{}] because it is not known", task, relationshipName, processor);
            return false;
        }

        final List<String> newRelationshipNames = new ArrayList<>(additionalRelationshipNames.length + 1);
        newRelationshipNames.add(firstRelationshipName);
        newRelationshipNames.addAll(Arrays.asList(additionalRelationshipNames));

        for (final String newName : newRelationshipNames) {
            if (relationships.contains(newName) && !newName.equals(relationshipName)) {
                throw new IllegalStateException("Cannot %s Relationship %s to %s for %s because a Relationship already exists with the name %s".formatted(
                    task, relationshipName, newRelationshipNames, processor, newName));
            }
        }

        final Relationship existingRelationship = processor.getRelationship(relationshipName);
        final Set<Relationship> newRelationships = new HashSet<>();
        for (final String newRelationshipName : newRelationshipNames) {
            final Relationship newRelationship = new Relationship.Builder()
                .name(newRelationshipName)
                .autoTerminateDefault(existingRelationship.isAutoTerminated())
                .description(existingRelationship.getDescription())
                .build();

            newRelationships.add(newRelationship);
        }

        // Update auto-terminated state
        final Set<Relationship> autoTerminated = new HashSet<>(processor.getAutoTerminatedRelationships());
        final boolean removed = autoTerminated.remove(existingRelationship);
        if (removed) {
            autoTerminated.addAll(newRelationships);
            processor.setAutoTerminatedRelationships(autoTerminated);
            logger.info("Removed {} from {}'s Auto-terminated Relationships and replaced with {}", existingRelationship, processor, newRelationships);
        } else {
            logger.debug("{} is not auto-terminated for {} so will not replace its auto-terminated Relationships", existingRelationship, processor);
        }

        // Update any connections
        final Set<Connection> connections = processor.getConnections(existingRelationship);
        if (connections.isEmpty()) {
            logger.debug("There are no outgoing connections from {} for {} so will not update any connections", existingRelationship, processor);
        } else {
            for (final Connection connection : connections) {
                final Set<Relationship> connectionRelationships = new HashSet<>(connection.getRelationships());
                connectionRelationships.remove(existingRelationship);
                connectionRelationships.addAll(newRelationships);

                connection.setRelationships(connectionRelationships);
                logger.info("Removed {} from {} and replaced with {}", existingRelationship, connection, newRelationshipNames);
            }
        }

        // Update retry state
        final Set<String> retriedRelationshipNames = new HashSet<>(processor.getRetriedRelationships());
        if (retriedRelationshipNames.remove(relationshipName)) {
            retriedRelationshipNames.addAll(newRelationshipNames);
            processor.setRetriedRelationships(retriedRelationshipNames);

            logger.info("Removed {} from {}'s list of auto-retried Relationships and replaced with {}", existingRelationship, processor, newRelationships);
        } else {
            logger.debug("{} was not automatically retried for {}", existingRelationship, processor);
        }

        return true;
    }


    @Override
    public boolean hasRelationship(final String relationshipName) {
        return relationships.contains(relationshipName);
    }

}
