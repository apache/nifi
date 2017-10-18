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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * <p>
 * This processor routes a FlowFile based on its flow file attributes by using the Attribute Expression Language. The Expression Language is used by adding Optional Properties to the processor. The
 * name of the Property indicates the name of the relationship to which a FlowFile will be routed if matched. The value of the Property indicates an Attribute Expression Language Expression that will
 * be used to determine whether or not a given FlowFile will be routed to the associated relationship. If multiple expressions match a FlowFile's attributes, that FlowFile will be cloned and routed to
 * each corresponding relationship. If none of the supplied expressions matches for a given FlowFile, that FlowFile will be routed to the 'unmatched' relationship.
 * </p>
 *
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "routing", "Attribute Expression Language", "regexp", "regex", "Regular Expression", "Expression Language"})
@CapabilityDescription("Routes FlowFiles based on their Attributes using the Attribute Expression Language")
@DynamicProperty(name = "Relationship Name", value = "Attribute Expression Language", supportsExpressionLanguage = true, description = "Routes FlowFiles whose "
        + "attributes match the Attribute Expression Language specified in the Dynamic Property Value to the Relationship specified in the Dynamic Property Key")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's Attribute Expression Language")
@WritesAttributes({
    @WritesAttribute(attribute = RouteOnAttribute.ROUTE_ATTRIBUTE_KEY, description = "The relation to which the FlowFile was routed")
})
public class RouteOnAttribute extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_KEY = "RouteOnAttribute.Route";

    // keep the word 'match' instead of 'matched' to maintain backward compatibility (there was a typo originally)
    private static final String routeAllMatchValue = "Route to 'match' if all match";
    private static final String routeAnyMatches = "Route to 'match' if any matches";
    private static final String routePropertyNameValue = "Route to Property name";

    public static final AllowableValue ROUTE_PROPERTY_NAME = new AllowableValue(routePropertyNameValue, "Route to Property name",
            "A copy of the FlowFile will be routed to each relationship whose corresponding expression evaluates to 'true'");
    public static final AllowableValue ROUTE_ALL_MATCH = new AllowableValue(routeAllMatchValue, "Route to 'matched' if all match",
            "Requires that all user-defined expressions evaluate to 'true' for the FlowFile to be considered a match");
    // keep the word 'match' instead of 'matched' to maintain backward compatibility (there was a typo originally)
    public static final AllowableValue ROUTE_ANY_MATCHES = new AllowableValue(routeAnyMatches,
            "Route to 'matched' if any matches",
            "Requires that at least one user-defined expression evaluate to 'true' for the FlowFile to be considered a match");

    public static final PropertyDescriptor ROUTE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Routing Strategy")
            .description("Specifies how to determine which relationship to use when evaluating the Expression Language")
            .required(true)
            .allowableValues(ROUTE_PROPERTY_NAME, ROUTE_ALL_MATCH, ROUTE_ANY_MATCHES)
            .defaultValue(ROUTE_PROPERTY_NAME.getValue())
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles that do not match any user-define expression will be routed here")
            .build();
    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles will be routed to 'match' if one or all Expressions match, depending on the configuration of the Routing Strategy property")
            .build();

    private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private List<PropertyDescriptor> properties;
    private volatile String configuredRouteStrategy = ROUTE_STRATEGY.getDefaultValue();
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    /**
     * Cache of dynamic properties set during {@link #onScheduled(ProcessContext)} for quick access in
     * {@link #onTrigger(ProcessContext, ProcessSession)}
     */
    private volatile Map<Relationship, PropertyValue> propertyMap = new HashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> set = new HashSet<>();
        set.add(REL_NO_MATCH);
        relationships = new AtomicReference<>(set);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ROUTE_STRATEGY);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.BOOLEAN, false))
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(ROUTE_STRATEGY)) {
            configuredRouteStrategy = newValue;
        } else {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }

            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }

        // formulate the new set of Relationships
        final Set<String> allDynamicProps = this.dynamicPropertyNames;
        final Set<Relationship> newRelationships = new HashSet<>();
        final String routeStrategy = configuredRouteStrategy;
        if (ROUTE_PROPERTY_NAME.equals(routeStrategy)) {
            for (final String propName : allDynamicProps) {
                newRelationships.add(new Relationship.Builder().name(propName).build());
            }
        } else {
            newRelationships.add(REL_MATCH);
        }

        newRelationships.add(REL_NO_MATCH);
        this.relationships.set(newRelationships);
    }

    /**
     * When this processor is scheduled, update the dynamic properties into the map
     * for quick access during each onTrigger call
     * @param context ProcessContext used to retrieve dynamic properties
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            getLogger().debug("Adding new dynamic property: {}", new Object[]{descriptor});
            newPropertyMap.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
        }

        this.propertyMap = newPropertyMap;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Map<Relationship, PropertyValue> propMap = this.propertyMap;
        final Set<Relationship> matchingRelationships = new HashSet<>();
        for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {
            final PropertyValue value = entry.getValue();

            final boolean matches = value.evaluateAttributeExpressions(flowFile).asBoolean();
            if (matches) {
                matchingRelationships.add(entry.getKey());
            }
        }

        final Set<Relationship> destinationRelationships = new HashSet<>();
        switch (context.getProperty(ROUTE_STRATEGY).getValue()) {
            case routeAllMatchValue:
                if (matchingRelationships.size() == propMap.size()) {
                    destinationRelationships.add(REL_MATCH);
                } else {
                    destinationRelationships.add(REL_NO_MATCH);
                }
                break;
            case routeAnyMatches:
                if (matchingRelationships.isEmpty()) {
                    destinationRelationships.add(REL_NO_MATCH);
                } else {
                    destinationRelationships.add(REL_MATCH);
                }
                break;
            case routePropertyNameValue:
            default:
                destinationRelationships.addAll(matchingRelationships);
                break;
        }

        if (destinationRelationships.isEmpty()) {
            logger.info("Routing {} to unmatched", new Object[]{ flowFile });
            flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, REL_NO_MATCH.getName());
            session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
            session.transfer(flowFile, REL_NO_MATCH);
        } else {
            final Iterator<Relationship> relationshipNameIterator = destinationRelationships.iterator();
            final Relationship firstRelationship = relationshipNameIterator.next();
            final Map<Relationship, FlowFile> transferMap = new HashMap<>();
            final Set<FlowFile> clones = new HashSet<>();

            // make all the clones for any remaining relationships
            while (relationshipNameIterator.hasNext()) {
                final Relationship relationship = relationshipNameIterator.next();
                final FlowFile cloneFlowFile = session.clone(flowFile);
                clones.add(cloneFlowFile);
                transferMap.put(relationship, cloneFlowFile);
            }

            // now transfer any clones generated
            for (final Map.Entry<Relationship, FlowFile> entry : transferMap.entrySet()) {
                logger.info("Cloned {} into {} and routing clone to relationship {}", new Object[]{ flowFile, entry.getValue(), entry.getKey() });
                FlowFile updatedFlowFile = session.putAttribute(entry.getValue(), ROUTE_ATTRIBUTE_KEY, entry.getKey().getName());
                session.getProvenanceReporter().route(updatedFlowFile, entry.getKey());
                session.transfer(updatedFlowFile, entry.getKey());
            }

            //now transfer the original flow file
            logger.info("Routing {} to {}", new Object[]{flowFile, firstRelationship});
            session.getProvenanceReporter().route(flowFile, firstRelationship);
            flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, firstRelationship.getName());
            session.transfer(flowFile, firstRelationship);
        }
    }
}
