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
package org.apache.nifi.processors.script;

import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Tags({"record", "partition", "script", "groovy", "jython", "python", "segment", "split", "group", "organize"})
@CapabilityDescription(
        "This processor provides the ability to partition and route the records of the incoming FlowFile using an user-provided script. " +
        "The script is expected to handle a record as argument and return with a string value. " +
        "The returned value defines which partition the given record should end up. " +
        "Partitions are defined as dynamic properties: dynamic property names are serving as partitions known by the processor. " +
        "The value of a dynamic property defines the relationship the given record will be routed into. Multiple partitons might point to the same relationship. " +
        "Creation of these dynamic relationship is managed by the processor. " +
        "The records, which for the script returned with an unknown relationship name are routed to the \"unmatched\" relationship. " +
        "The records are batched: for an incoming FlowFile, all the records routed towards a given relationship are batched into one single FlowFile."
)
@SeeAlso(classNames = {
    "org.apache.nifi.processors.script.ScriptedTransformRecord",
    "org.apache.nifi.processors.script.ScriptedValidateRecord",
    "org.apache.nifi.processors.script.ScriptedFilterRecord"
})
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's Attribute Expression Language")
public class ScriptedPartitionRecord extends ScriptedRouterProcessor<String> {

    static final Relationship RELATIONSHIP_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description(
                "After successful procession, the incoming FlowFile will be transferred to this relationship. " +
                "This happens regardless the records are matching to a relationship or not.")
            .build();

    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failed")
            .description("In case of any issue during processing the incoming FlowFile, the incoming FlowFile will be routed to this relationship.")
            .build();

    static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("unmatched")
            .description("Records where the script evaluation returns with an unknown partition are routed to this relationship.")
            .build();

    private static Set<Relationship> RELATIONSHIPS = new HashSet<>();

    static {
        RELATIONSHIPS.add(RELATIONSHIP_ORIGINAL);
        RELATIONSHIPS.add(RELATIONSHIP_FAILURE);
        RELATIONSHIPS.add(RELATIONSHIP_SUCCESS);
    }

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private final Map<String, Relationship> partitions = new ConcurrentHashMap<>();

    public ScriptedPartitionRecord() {
        super(String.class);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships.set(new HashSet<>(RELATIONSHIPS));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .required(false)
            .name(propertyDescriptorName)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamic(true)
            .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Set<Relationship> relationships = new HashSet<>(this.relationships.get());
            final String partitionName = descriptor.getName();

            if (shouldDeleteDynamicRelationship(partitionName, oldValue)) {
                relationships.remove(new Relationship.Builder().name(oldValue).build());
            }

            if (newValue == null) {
                partitions.remove(partitionName);
            } else {
                final Relationship newRelationship = new Relationship.Builder().name(newValue).build();
                partitions.put(partitionName, newRelationship);
                relationships.add(newRelationship);
            }

            this.relationships.set(relationships);
        }
    }

    private boolean shouldDeleteDynamicRelationship(final String partitionName, final String oldValue) {
        // If no further partition points to the same relationship and it is not a static relationship, it must be removed
        final Set<String> staticRelationships = RELATIONSHIPS.stream().map(r -> r.getName()).collect(Collectors.toSet());
        return !partitions.entrySet().stream().filter(e -> !e.getKey().equals(partitionName)).map(e -> e.getValue().getName()).collect(Collectors.toSet()).contains(oldValue)
            && !staticRelationships.contains(oldValue);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final Map<PropertyDescriptor, String> properties = validationContext.getProperties();
        final Set<String> staticRelationships = RELATIONSHIPS.stream().map(r -> r.getName()).collect(Collectors.toSet());

        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            if (entry.getKey().isDynamic() && staticRelationships.contains(entry.getValue())) {
                results.add(new ValidationResult.Builder()
                    .subject("DynamicRelationships")
                    .valid(false)
                    .explanation("partition " + entry.getKey().getDisplayName() + " cannot point to any static relationship!")
                    .build());
            }
        }

        return results;
    }

    @Override
    protected Relationship getOriginalRelationship() {
        return RELATIONSHIP_ORIGINAL;
    }

    @Override
    protected Relationship getFailedRelationship() {
        return RELATIONSHIP_FAILURE;
    }

    @Override
    protected Optional<Relationship> resolveRelationship(final String scriptResult) {
        return partitions.containsKey(scriptResult)
            ? Optional.of(partitions.get(scriptResult))
            : Optional.of(RELATIONSHIP_SUCCESS);
    }
}
