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

package org.apache.nifi.components.connector.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"test", "duplicate", "connector"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Duplicates incoming FlowFiles to numbered relationships based on the configured number of duplicates. " +
    "Each duplicate is sent to a relationship numbered 1, 2, 3, etc., up to the configured number.")
public class DuplicateFlowFile extends AbstractProcessor {

    public static final PropertyDescriptor NUM_DUPLICATES = new PropertyDescriptor.Builder()
        .name("Number of Duplicates")
        .description("Specifies how many duplicates of each incoming FlowFile will be created")
        .required(true)
        .defaultValue("3")
        .addValidator(StandardValidators.createLongValidator(1L, 10L, true))
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(NUM_DUPLICATES);

    private final AtomicReference<Set<Relationship>> relationshipsRef = new AtomicReference<>();
    private final AtomicReference<List<Relationship>> sortedRelationshipsRef = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = relationshipsRef.get();
        return relationships == null ? Set.of() : relationships;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (NUM_DUPLICATES.equals(descriptor)) {
            updateRelationships(newValue);
        }
    }

    private void updateRelationships(final String numDuplicatesValue) {
        if (numDuplicatesValue == null) {
            relationshipsRef.set(Set.of());
            sortedRelationshipsRef.set(List.of());
            return;
        }

        try {
            final int numDuplicates = Integer.parseInt(numDuplicatesValue);
            final Set<Relationship> relationships = new HashSet<>();
            final List<Relationship> sortedRelationships = new ArrayList<>();

            for (int i = 1; i <= numDuplicates; i++) {
                final Relationship relationship = new Relationship.Builder()
                    .name(String.valueOf(i))
                    .description("Relationship for duplicate " + i)
                    .build();
                relationships.add(relationship);
                sortedRelationships.add(relationship);
            }

            relationshipsRef.set(relationships);
            sortedRelationshipsRef.set(sortedRelationships);
        } catch (final NumberFormatException e) {
            relationshipsRef.set(Set.of());
            sortedRelationshipsRef.set(List.of());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<Relationship> sortedRelationships = sortedRelationshipsRef.get();

        if (sortedRelationships == null || sortedRelationships.isEmpty()) {
            session.rollback();
            return;
        }

        // Create duplicates and send to numbered relationships
        for (final Relationship relationship : sortedRelationships) {
            final FlowFile duplicate = session.clone(flowFile);
            session.transfer(duplicate, relationship);
        }

        // Remove the original FlowFile
        session.remove(flowFile);
    }
}
