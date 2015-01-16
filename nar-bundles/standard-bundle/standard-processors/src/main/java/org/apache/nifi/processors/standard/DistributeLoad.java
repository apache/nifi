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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.loading.LoadDistributionListener;
import org.apache.nifi.loading.LoadDistributionService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.EventDriven;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.annotation.TriggerWhenAnyDestinationAvailable;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.commons.lang3.StringUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@TriggerWhenAnyDestinationAvailable
@Tags({"distribute", "load balance", "route", "round robin", "weighted"})
@CapabilityDescription("Distributes FlowFiles to downstream processors based on a Distribution Strategy. If using the Round Robin "
        + "strategy, the default is to assign each destination a weighting of 1 (evenly distributed). However, optional properties"
        + "can be added to the change this; adding a property with the name '5' and value '10' means that the relationship with name "
        + "'5' will be receive 10 FlowFiles in each iteration instead of 1.")
public class DistributeLoad extends AbstractProcessor {

    public static final String STRATEGY_ROUND_ROBIN = "round robin";
    public static final String STRATEGY_NEXT_AVAILABLE = "next available";
    public static final String STRATEGY_LOAD_DISTRIBUTION_SERVICE = "load distribution service";

    public static final PropertyDescriptor NUM_RELATIONSHIPS = new PropertyDescriptor.Builder()
            .name("Number of Relationships")
            .description("Determines the number of Relationships to which the load should be distributed")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor DISTRIBUTION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Distribution Strategy")
            .description(
                    "Determines how the load will be distributed. If using Round Robin, will not distribute any FlowFiles unless all destinations can accept FlowFiles; when using Next Available, will distribute FlowFiles as long as at least 1 destination can accept FlowFiles.")
            .required(true)
            .allowableValues(STRATEGY_ROUND_ROBIN, STRATEGY_NEXT_AVAILABLE, STRATEGY_LOAD_DISTRIBUTION_SERVICE)
            .defaultValue(STRATEGY_ROUND_ROBIN)
            .build();

    public static final PropertyDescriptor HOSTNAMES = new PropertyDescriptor.Builder()
            .name("Hostnames")
            .description("List of remote servers to distribute across. Each server must be FQDN and use either ',', ';', or [space] as a delimiter")
            .required(true)
            .addValidator(new Validator() {

                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                    ValidationResult result = new ValidationResult.Builder()
                    .subject(subject)
                    .valid(true)
                    .input(input)
                    .explanation("Good FQDNs")
                    .build();
                    if (null == input) {
                        result = new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Need to specify delimited list of FQDNs")
                        .build();
                        return result;
                    }
                    String[] hostNames = input.split("(?:,+|;+|\\s+)");
                    for (String hostName : hostNames) {
                        if (StringUtils.isNotBlank(hostName) && !hostName.contains(".")) {
                            result = new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Need a FQDN rather than a simple host name.")
                            .build();
                            return result;
                        }
                    }
                    return result;
                }
            })
            .build();
    public static final PropertyDescriptor LOAD_DISTRIBUTION_SERVICE_TEMPLATE = new PropertyDescriptor.Builder()
            .name("Load Distribution Service ID")
            .description("The identifier of the Load Distribution Service")
            .required(true)
            .identifiesControllerService(LoadDistributionService.class)
            .build();

    private List<PropertyDescriptor> properties;
    private final AtomicReference<Set<Relationship>> relationshipsRef = new AtomicReference<>();
    private final AtomicReference<DistributionStrategy> strategyRef = new AtomicReference<DistributionStrategy>(new RoundRobinStrategy());
    private final AtomicReference<List<Relationship>> weightedRelationshipListRef = new AtomicReference<>();
    private final AtomicBoolean doCustomValidate = new AtomicBoolean(false);
    private volatile LoadDistributionListener myListener;
    private final AtomicBoolean doSetProps = new AtomicBoolean(true);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(createRelationship(1));
        relationshipsRef.set(Collections.unmodifiableSet(relationships));

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(NUM_RELATIONSHIPS);
        properties.add(DISTRIBUTION_STRATEGY);
        this.properties = Collections.unmodifiableList(properties);
    }

    private static Relationship createRelationship(final int num) {
        return new Relationship.Builder().name(String.valueOf(num)).build();
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
            this.relationshipsRef.set(Collections.unmodifiableSet(relationships));
        } else if (descriptor.equals(DISTRIBUTION_STRATEGY)) {
            switch (newValue.toLowerCase()) {
                case STRATEGY_ROUND_ROBIN:
                    strategyRef.set(new RoundRobinStrategy());
                    break;
                case STRATEGY_NEXT_AVAILABLE:
                    strategyRef.set(new NextAvailableStrategy());
                    break;
                case STRATEGY_LOAD_DISTRIBUTION_SERVICE:
                    strategyRef.set(new LoadDistributionStrategy());
            }
            doSetProps.set(true);
            doCustomValidate.set(true);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        if (strategyRef.get() instanceof LoadDistributionStrategy && doSetProps.getAndSet(false)) {
            final List<PropertyDescriptor> props = new ArrayList<>(properties);
            props.add(LOAD_DISTRIBUTION_SERVICE_TEMPLATE);
            props.add(HOSTNAMES);
            this.properties = Collections.unmodifiableList(props);
        } else if (doSetProps.getAndSet(false)) {
            final List<PropertyDescriptor> props = new ArrayList<>();
            props.add(NUM_RELATIONSHIPS);
            props.add(DISTRIBUTION_STRATEGY);
            this.properties = Collections.unmodifiableList(props);
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
                return new PropertyDescriptor.Builder().addValidator(new InvalidPropertyNameValidator(propertyDescriptorName))
                        .name(propertyDescriptorName).build();
            }
        } catch (final NumberFormatException e) {
            return new PropertyDescriptor.Builder().addValidator(new InvalidPropertyNameValidator(propertyDescriptorName))
                    .name(propertyDescriptorName).build();
        }

        // validate that the property value is valid
        return new PropertyDescriptor.Builder().addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .name(propertyDescriptorName).dynamic(true).build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> results = new ArrayList<>();
        if (doCustomValidate.getAndSet(false)) {
            String distStrat = validationContext.getProperty(DISTRIBUTION_STRATEGY).getValue();
            if (distStrat.equals(STRATEGY_LOAD_DISTRIBUTION_SERVICE)) {
                // make sure Hostnames and Controller service are set
                PropertyValue propDesc = validationContext.getProperty(HOSTNAMES);
                if (null == propDesc || null == propDesc.getValue() || propDesc.getValue().isEmpty()) {
                    results.add(new ValidationResult.Builder()
                            .subject(HOSTNAMES.getName())
                            .explanation("Must specify Hostnames when using 'Load Distribution Strategy'")
                            .valid(false)
                            .build());
                }
                propDesc = validationContext.getProperty(LOAD_DISTRIBUTION_SERVICE_TEMPLATE);
                if (null == propDesc || null == propDesc.getValue() || propDesc.getValue().isEmpty()) {
                    results.add(new ValidationResult.Builder()
                            .subject(LOAD_DISTRIBUTION_SERVICE_TEMPLATE.getName())
                            .explanation("Must specify 'Load Distribution Service ID' when using 'Load Distribution Strategy'")
                            .valid(false)
                            .build());
                }
                if (results.isEmpty()) {
                    int numRels = validationContext.getProperty(NUM_RELATIONSHIPS).asInteger();
                    String hostNamesValue = validationContext.getProperty(HOSTNAMES).getValue();
                    String[] hostNames = hostNamesValue.split("(?:,+|;+|\\s+)");
                    int numHosts = 0;
                    for (String hostName : hostNames) {
                        if (StringUtils.isNotBlank(hostName)) {
                            hostNames[numHosts++] = hostName;
                        }
                    }
                    if (numHosts > numRels) {
                        results.add(new ValidationResult.Builder()
                                .subject("Number of Relationships and Hostnames")
                                .explanation("Number of Relationships must be equal to, or greater than, the number of host names")
                                .valid(false)
                                .build());
                    } else {
                        // create new relationships with descriptions of hostname
                        Set<Relationship> relsWithDesc = new TreeSet<>();
                        for (int i = 0; i < numHosts; i++) {
                            relsWithDesc.add(new Relationship.Builder().name(String.valueOf(i + 1)).description(hostNames[i]).build());
                        }
                        // add add'l rels if configuration requires it...it probably shouldn't
                        for (int i = numHosts + 1; i <= numRels; i++) {
                            relsWithDesc.add(createRelationship(i));
                        }
                        relationshipsRef.set(Collections.unmodifiableSet(relsWithDesc));
                    }
                }
            }
        }
        return results;
    }

    @OnScheduled
    public void createWeightedList(final ProcessContext context) {
        final Map<Integer, Integer> weightings = new LinkedHashMap<>();

        String distStrat = context.getProperty(DISTRIBUTION_STRATEGY).getValue();
        if (distStrat.equals(STRATEGY_LOAD_DISTRIBUTION_SERVICE)) {
            String hostNamesValue = context.getProperty(HOSTNAMES).getValue();
            String[] hostNames = hostNamesValue.split("(?:,+|;+|\\s+)");
            Set<String> hostNameSet = new HashSet<>();
            for (String hostName : hostNames) {
                if (StringUtils.isNotBlank(hostName)) {
                    hostNameSet.add(hostName);
                }
            }
            LoadDistributionService svc = context.getProperty(LOAD_DISTRIBUTION_SERVICE_TEMPLATE).asControllerService(LoadDistributionService.class);
            myListener = new LoadDistributionListener() {

                @Override
                public void update(Map<String, Integer> loadInfo) {
                    for (Relationship rel : relationshipsRef.get()) {
                        String hostname = rel.getDescription();
                        Integer weight = 1;
                        if (loadInfo.containsKey(hostname)) {
                            weight = loadInfo.get(hostname);
                        }
                        weightings.put(Integer.decode(rel.getName()), weight);
                    }
                    updateWeightedRelationships(weightings);
                }
            };

            Map<String, Integer> loadInfo = svc.getLoadDistribution(hostNameSet, myListener);
            for (Relationship rel : relationshipsRef.get()) {
                String hostname = rel.getDescription();
                Integer weight = 1;
                if (loadInfo.containsKey(hostname)) {
                    weight = loadInfo.get(hostname);
                }
                weightings.put(Integer.decode(rel.getName()), weight);
            }

        } else {
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

        this.weightedRelationshipListRef.set(Collections.unmodifiableList(relationshipList));
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
            return;
        }

        final Relationship relationship = strategy.mapToRelationship(context, flowFile);
        if (relationship == null) {
            // can't transfer the FlowFiles. Roll back and yield
            session.rollback();
            context.yield();
            return;
        }

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
                    .valid(false)
                    .build();
        }
    }

    /**
     * Implementations must be thread-safe.
     */
    private static interface DistributionStrategy {

        /**
         * Returns a mapping of FlowFile to Relationship or <code>null</code> if
         * the needed relationships are not available to accept files.
         *
         * @param session
         * @param flowFiles
         * @return
         */
        Relationship mapToRelationship(ProcessContext context, FlowFile flowFile);

        boolean requiresAllDestinationsAvailable();
    }

    private class LoadDistributionStrategy implements DistributionStrategy {

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

    private class RoundRobinStrategy implements DistributionStrategy {

        private final AtomicLong counter = new AtomicLong(0L);

        @Override
        public Relationship mapToRelationship(final ProcessContext context, final FlowFile flowFile) {
            final List<Relationship> relationshipList = DistributeLoad.this.weightedRelationshipListRef.get();
            final long counterValue = counter.getAndIncrement();
            final int idx = (int) (counterValue % relationshipList.size());
            final Relationship relationship = relationshipList.get(idx);
            return relationship;
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
}
