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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(Requirement.INPUT_REQUIRED)
@TriggerWhenAnyDestinationAvailable
@Tags({"distribute", "load balance", "route", "round robin", "weighted"})
@CapabilityDescription("Distributes FlowFiles to downstream processors based on a Distribution Strategy. If using the Round Robin "
        + "strategy, the default is to assign each destination a weighting of 1 (evenly distributed). However, optional properties "
        + "can be added to the change this; adding a property with the name '5' and value '10' means that the relationship with name "
        + "'5' will be receive 10 FlowFiles in each iteration instead of 1.")
@DynamicProperty(name = "The relationship name (positive number)", value = "The relationship Weight (positive number)", description = "Adding a "
        + "property with the name '5' and value '10' means that the relationship with name "
        + "'5' will receive 10 FlowFiles in each iteration instead of 1.")
@DynamicRelationship(name = "A number 1..<Number Of Relationships>", description = "FlowFiles are sent to this relationship per the "
        + "<Distribution Strategy>")
@WritesAttributes(
        @WritesAttribute(attribute = "distribute.load.relationship", description = "The name of the specific relationship the FlowFile has been routed through")
)
public class DistributeLoad extends AbstractProcessor {

    public static final String ROUND_ROBIN = "round robin";
    public static final String NEXT_AVAILABLE = "next available";
    public static final String OVERFLOW = "overflow";

    public static final AllowableValue STRATEGY_ROUND_ROBIN = new AllowableValue(ROUND_ROBIN, ROUND_ROBIN,
            "Relationship selection is evenly distributed in a round robin fashion; all relationships must be available.");
    public static final AllowableValue STRATEGY_NEXT_AVAILABLE = new AllowableValue(NEXT_AVAILABLE, NEXT_AVAILABLE,
            "Relationship selection is distributed across all available relationships in order of their weight; at least one relationship must be available.");
    public static final AllowableValue STRATEGY_OVERFLOW = new AllowableValue(OVERFLOW, OVERFLOW,
            "Relationship selection is the first available relationship without further distribution among all relationships; at least one relationship must be available.");

    public static final PropertyDescriptor NUM_RELATIONSHIPS = new PropertyDescriptor.Builder()
            .name("Number of Relationships")
            .description("Determines the number of Relationships to which the load should be distributed")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor DISTRIBUTION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Distribution Strategy")
            .description("Determines how the load will be distributed. Relationship weight is in numeric order where '1' has the greatest weight.")
            .required(true)
            .allowableValues(STRATEGY_ROUND_ROBIN, STRATEGY_NEXT_AVAILABLE, STRATEGY_OVERFLOW)
            .defaultValue(ROUND_ROBIN)
            .build();

    private List<PropertyDescriptor> properties = List.of(
            NUM_RELATIONSHIPS,
            DISTRIBUTION_STRATEGY
    );

    public static final String RELATIONSHIP_ATTRIBUTE = "distribute.load.relationship";

    private final AtomicReference<Set<Relationship>> relationshipsRef = new AtomicReference<>();
    private final AtomicReference<DistributionStrategy> strategyRef = new AtomicReference<>(new RoundRobinStrategy());
    private final AtomicReference<List<Relationship>> weightedRelationshipListRef = new AtomicReference<>();
    private final AtomicBoolean doSetProps = new AtomicBoolean(true);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationshipsRef.set(Set.of(createRelationship(1)));
    }

    private static Relationship createRelationship(final int num) {
        return new Relationship.Builder().name(String.valueOf(num))
                .description("Where to route flowfiles for this relationship index").build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationshipsRef.get();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(NUM_RELATIONSHIPS)) {
            final Set<Relationship> relationships = new HashSet<>();
            for (int i = 1; i <= Integer.parseInt(newValue); i++) {
                relationships.add(createRelationship(i));
            }
            this.relationshipsRef.set(Set.copyOf(relationships));
        } else if (descriptor.equals(DISTRIBUTION_STRATEGY)) {
            switch (newValue.toLowerCase()) {
                case ROUND_ROBIN:
                    strategyRef.set(new RoundRobinStrategy());
                    break;
                case NEXT_AVAILABLE:
                    strategyRef.set(new NextAvailableStrategy());
                    break;
                case OVERFLOW:
                    strategyRef.set(new OverflowStrategy());
                    break;
                default:
                    throw new IllegalStateException("Invalid distribution strategy");
            }
            doSetProps.set(true);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
       if (doSetProps.getAndSet(false)) {
           this.properties = List.of(NUM_RELATIONSHIPS, DISTRIBUTION_STRATEGY);
        }
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        // validate that the property name is valid.
        final int numRelationships = this.relationshipsRef.get().size();
        try {
            final int value = Integer.parseInt(propertyDescriptorName);
            if (value <= 0 || value > numRelationships) {
                return new PropertyDescriptor.Builder()
                        .addValidator(new InvalidPropertyNameValidator(propertyDescriptorName)).name(propertyDescriptorName).build();
            }
        } catch (final NumberFormatException e) {
            return new PropertyDescriptor.Builder()
                    .addValidator(new InvalidPropertyNameValidator(propertyDescriptorName)).name(propertyDescriptorName).build();
        }

        // validate that the property value is valid
        return new PropertyDescriptor.Builder().addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .name(propertyDescriptorName).dynamic(true).build();
    }

    @OnScheduled
    public void createWeightedList(final ProcessContext context) {
        final Map<Integer, Integer> weightings = new LinkedHashMap<>();

        final int numRelationships = context.getProperty(NUM_RELATIONSHIPS).asInteger();
        for (int i = 1; i <= numRelationships; i++) {
            weightings.put(i, 1);
        }
        for (final PropertyDescriptor propDesc : context.getProperties().keySet()) {
            if (!this.properties.contains(propDesc)) {
                final int relationship = Integer.parseInt(propDesc.getName());
                final int weighting = context.getProperty(propDesc).asInteger();
                weightings.put(relationship, weighting);
            }
        }

        updateWeightedRelationships(weightings);
    }

    private void updateWeightedRelationships(final Map<Integer, Integer> weightings) {
        final List<Relationship> relationshipList = new ArrayList<>();
        for (final Map.Entry<Integer, Integer> entry : weightings.entrySet()) {
            final String relationshipName = String.valueOf(entry.getKey());
            final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
            for (int i = 0; i < entry.getValue(); i++) {
                relationshipList.add(relationship);
            }
        }

        this.weightedRelationshipListRef.set(List.copyOf(relationshipList));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final DistributionStrategy strategy = strategyRef.get();
        final Set<Relationship> available = context.getAvailableRelationships();
        final int numRelationships = context.getProperty(NUM_RELATIONSHIPS).asInteger();
        final boolean allDestinationsAvailable = (available.size() == numRelationships);
        if (!allDestinationsAvailable && strategy.requiresAllDestinationsAvailable()) {
            // can't transfer the FlowFiles. Roll back and yield
            session.rollback();
            context.yield();
            return;
        }

        final Relationship relationship = strategy.mapToRelationship(context, flowFile);
        if (relationship == null) {
            // can't transfer the FlowFiles. Roll back and yield
            session.rollback();
            context.yield();
            return;
        }

        // add an attribute capturing which relationship a flowfile was routed through
        session.putAttribute(flowFile, RELATIONSHIP_ATTRIBUTE, relationship.getName());

        session.transfer(flowFile, relationship);
        session.getProvenanceReporter().route(flowFile, relationship);
    }

    private static class InvalidPropertyNameValidator implements Validator {

        private final String propertyName;

        public InvalidPropertyNameValidator(final String propertyName) {
            this.propertyName = propertyName;
        }

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            return new ValidationResult.Builder().subject("Property Name").input(propertyName)
                    .explanation("Property Name must be a positive integer between 1 and the number of relationships (inclusive)")
                    .valid(false).build();
        }
    }

    /**
     * Implementations must be thread-safe.
     */
    private interface DistributionStrategy {

        /**
         * @param context context
         * @param flowFile flowFile
         * @return a mapping of FlowFile to Relationship or <code>null</code> if the needed relationships are not available to accept files
         */
        Relationship mapToRelationship(ProcessContext context, FlowFile flowFile);

        boolean requiresAllDestinationsAvailable();
    }

    private class RoundRobinStrategy implements DistributionStrategy {

        private final AtomicLong counter = new AtomicLong(0L);

        @Override
        public Relationship mapToRelationship(final ProcessContext context, final FlowFile flowFile) {
            final List<Relationship> relationshipList = DistributeLoad.this.weightedRelationshipListRef.get();
            final long counterValue = counter.getAndIncrement();
            final int idx = (int) (counterValue % relationshipList.size());
            return relationshipList.get(idx);
        }

        @Override
        public boolean requiresAllDestinationsAvailable() {
            return true;
        }
    }

    private class NextAvailableStrategy implements DistributionStrategy {

        private final AtomicLong counter = new AtomicLong(0L);

        @Override
        public Relationship mapToRelationship(final ProcessContext context, final FlowFile flowFile) {
            final List<Relationship> relationshipList = DistributeLoad.this.weightedRelationshipListRef.get();
            final int numRelationships = relationshipList.size();

            // create a HashSet that contains all of the available relationships, as calling #contains on HashSet
            // is much faster than calling it on a List
            boolean foundFreeRelationship = false;
            Relationship relationship = null;

            int attempts = 0;
            while (!foundFreeRelationship) {
                final long counterValue = counter.getAndIncrement();
                final int idx = (int) (counterValue % numRelationships);
                relationship = relationshipList.get(idx);
                foundFreeRelationship = context.getAvailableRelationships().contains(relationship);
                if (++attempts % numRelationships == 0 && !foundFreeRelationship) {
                    return null;
                }
            }

            return relationship;
        }

        @Override
        public boolean requiresAllDestinationsAvailable() {
            return false;
        }
    }

    private class OverflowStrategy implements DistributionStrategy {

        @Override
        public Relationship mapToRelationship(final ProcessContext context, final FlowFile flowFile) {
            final List<Relationship> relationshipList = DistributeLoad.this.weightedRelationshipListRef.get();
            final int numRelationships = relationshipList.size();

            boolean foundFreeRelationship = false;
            Relationship relationship = null;
            // Getting set of available relationships only once. This may miss a relationship that recently became available, but
            // overall is more efficient than re-calling for every relationship evaluation
            Set<Relationship> availableRelationships = context.getAvailableRelationships();

            int weightedIndex = 0;
            while (!foundFreeRelationship) {
                relationship = relationshipList.get(weightedIndex);
                foundFreeRelationship = availableRelationships.contains(relationship);
                if (++weightedIndex % numRelationships == 0 && !foundFreeRelationship) {
                    return null;
                }
            }

            return relationship;
        }

        @Override
        public boolean requiresAllDestinationsAvailable() {
            return false;
        }
    }
}
