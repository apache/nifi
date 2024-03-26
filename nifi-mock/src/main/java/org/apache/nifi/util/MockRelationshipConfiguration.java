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
package org.apache.nifi.util;

import org.apache.nifi.migration.RelationshipConfiguration;
import org.apache.nifi.processor.Relationship;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MockRelationshipConfiguration implements RelationshipConfiguration {
    private final Map<String, Set<String>> relationshipSplits = new HashMap<>();
    private final Map<String, String> relationshipRenames = new HashMap<>();
    private final Map<String, Relationship> rawRelationships;
    private final Set<Relationship> originalRelationships;

    public MockRelationshipConfiguration(final Set<Relationship> relationships) {
        this.originalRelationships = Collections.unmodifiableSet(relationships);
        this.rawRelationships = relationships.stream().collect(Collectors.toMap(Relationship::getName, Function.identity()));
    }

    public RelationshipMigrationResult toRelationshipMigrationResult() {
        return new RelationshipMigrationResult() {
            @Override
            public Map<String, Set<String>> getPreviousRelationships() {
                return Collections.unmodifiableMap(relationshipSplits);
            }

            @Override
            public Map<String, String> getRenamedRelationships() {
                return Collections.unmodifiableMap(relationshipRenames);
            }
        };
    }

    @Override
    public boolean renameRelationship(final String relationshipName, final String newName) {
        relationshipRenames.put(relationshipName, newName);

        final boolean hasRelationship = hasRelationship(relationshipName);
        if (!hasRelationship) {
            return false;
        }

        rawRelationships.remove(relationshipName);
        rawRelationships.put(relationshipName, findRelationshipByName(newName));
        return true;
    }

    @Override
    public boolean splitRelationship(final String relationshipName, final String newRelationshipName, final String... additionalRelationshipNames) {
        final Set<String> newRelationships = new HashSet<>();
        newRelationships.add(newRelationshipName);
        if (additionalRelationshipNames != null) {
            newRelationships.addAll(Arrays.stream(additionalRelationshipNames).toList());
        }
        relationshipSplits.put(relationshipName, newRelationships);

        final boolean hasRelationship = hasRelationship(relationshipName);
        if (!hasRelationship) {
            return false;
        }

        rawRelationships.remove(relationshipName);
        newRelationships.forEach(r -> rawRelationships.put(r, findRelationshipByName(r)));
        return true;
    }

    @Override
    public boolean hasRelationship(final String relationshipName) {
        return rawRelationships.containsKey(relationshipName);
    }

    private Relationship findRelationshipByName(final String name) {
        return originalRelationships.stream().filter(r -> name.equals(r.getName())).findFirst().orElseThrow();
    }

    public Set<Relationship> getRawRelationships() {
        return Set.copyOf(rawRelationships.values());
    }
}
