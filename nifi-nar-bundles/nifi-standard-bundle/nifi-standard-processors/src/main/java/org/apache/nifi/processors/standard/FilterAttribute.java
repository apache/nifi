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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "filter", "retain", "remove", "delete", "regex", "regular expression", "Attribute Expression Language"})
@CapabilityDescription("Filters the Attributes of a FlowFile according to a specified strategy")
public class FilterAttribute extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All successful FlowFiles are routed to this relationship").name("success").build();

    private final static Set<Relationship> relationships = Collections.singleton(REL_SUCCESS);


    public static final AllowableValue FILTER_MODE_VALUE_RETAIN = new AllowableValue(
            "RETAIN",
            "Retain",
            "Retains only the attributes matching the filter, all other attributes are removed."
    );

    public static final AllowableValue FILTER_MODE_VALUE_REMOVE = new AllowableValue(
            "REMOVE",
            "Remove",
            "Removes the attributes matching the filter, all other attributes are retained."
    );

    public static final PropertyDescriptor FILTER_MODE = new PropertyDescriptor.Builder()
            .name("FILTER_MODE")
            .displayName("Filter mode")
            .description("Specifies the strategy to apply on filtered attributes. Either 'Remove' or 'Retain' only the matching attributes.")
            .required(true)
            .allowableValues(FILTER_MODE_VALUE_RETAIN, FILTER_MODE_VALUE_REMOVE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(FILTER_MODE_VALUE_RETAIN.getValue())
            .build();

    public static final AllowableValue MATCHING_STRATEGY_VALUE_ENUMERATION = new AllowableValue(
            "ENUMERATION",
            "Enumerate attributes",
            "Provides a set of attribute keys to filter for, separated by a comma delimiter ','."
    );

    public static final AllowableValue MATCHING_STRATEGY_VALUE_REGEX = new AllowableValue(
            "REGEX",
            "Use regular expression",
            "Provides a regular expression to match keys of attributes to filter for."
    );

    public static final PropertyDescriptor MATCHING_STRATEGY = new PropertyDescriptor.Builder()
            .name("MATCHING_STRATEGY")
            .displayName("Attribute matching strategy")
            .description("Specifies the strategy to filter attributes by.")
            .required(true)
            .allowableValues(MATCHING_STRATEGY_VALUE_ENUMERATION, MATCHING_STRATEGY_VALUE_REGEX)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue(MATCHING_STRATEGY_VALUE_ENUMERATION.getValue())
            .build();

    public static final PropertyDescriptor ATTRIBUTE_SET = new PropertyDescriptor.Builder()
            .name("ATTRIBUTE_SET")
            .displayName("Set of attributes to filter")
            .description("A set of attribute names to filter from FlowFiles. Each attribute name is separated by the comma delimiter ','.")
            .required(true)
            .dependsOn(MATCHING_STRATEGY, MATCHING_STRATEGY_VALUE_ENUMERATION)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_REGEX = new PropertyDescriptor.Builder()
            .name("ATTRIBUTE_REGEX")
            .displayName("Regular expression to filter attributes")
            .description("A regular expression to match names of attributes to filter from FlowFiles.")
            .required(true)
            .dependsOn(MATCHING_STRATEGY, MATCHING_STRATEGY_VALUE_REGEX)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_WITH_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private final static String DELIMITER_VALUE = ",";

    private final static List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(FILTER_MODE);
        _propertyDescriptors.add(MATCHING_STRATEGY);
        _propertyDescriptors.add(ATTRIBUTE_SET);
        _propertyDescriptors.add(ATTRIBUTE_REGEX);
        properties = Collections.unmodifiableList(_propertyDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private volatile Predicate<String> cachedMatchingPredicate;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final MatchingStrategy matchingStrategy = getMatchingStrategy(context);

        cachedMatchingPredicate = null;

        if (matchingStrategy == MatchingStrategy.ENUMERATION
                && !context.getProperty(ATTRIBUTE_SET).isExpressionLanguagePresent()) {
            cachedMatchingPredicate = determineMatchingPredicateBasedOnEnumeration(context, null);
        }
        if (matchingStrategy == MatchingStrategy.REGEX
                && !context.getProperty(ATTRIBUTE_REGEX).isExpressionLanguagePresent()) {
            cachedMatchingPredicate = determineMatchingPredicateBasedOnRegex(context, null);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Predicate<String> matchingPredicate = determineMatchingPredicate(context, flowFile);

        final FilterMode filterMode = getFilterMode(context, flowFile);
        final Predicate<String> isMatched;
        if (filterMode == FilterMode.RETAIN) {
            isMatched = matchingPredicate;
        } else {
            isMatched = matchingPredicate.negate();
        }

        final Set<String> attributesToRemove = new HashSet<>(flowFile.getAttributes().keySet());
        attributesToRemove.removeIf(isMatched);

        final FlowFile updatedFlowFile = session.removeAllAttributes(flowFile, attributesToRemove);
        session.transfer(updatedFlowFile, REL_SUCCESS);
    }

    private Predicate<String> determineMatchingPredicate(ProcessContext context, FlowFile flowFile) {
        if (cachedMatchingPredicate != null) {
            return cachedMatchingPredicate;
        }

        final MatchingStrategy matchingStrategy = getMatchingStrategy(context);

        switch (matchingStrategy) {
            case ENUMERATION:
                return determineMatchingPredicateBasedOnEnumeration(context, flowFile);
            case REGEX:
                return determineMatchingPredicateBasedOnRegex(context, flowFile);
            default:
                throw new IllegalArgumentException(
                        "Cannot determine matching predicate for unsupported strategy " + matchingStrategy + " !"
                );
        }
    }

    private static Predicate<String> determineMatchingPredicateBasedOnEnumeration(ProcessContext context, FlowFile flowFile) {
        final String attributeSetDeclaration = getAttributeSet(context, flowFile);
        final String delimiter = getDelimiter();

        Set<String> attributeSet = Arrays.stream(attributeSetDeclaration.split(Pattern.quote(delimiter)))
                .map(String::trim)
                .filter(attributeName -> !attributeName.trim().isEmpty())
                .collect(Collectors.toSet());

        return attributeSet::contains;
    }

    private static Predicate<String> determineMatchingPredicateBasedOnRegex(ProcessContext context, FlowFile flowFile) {
        Pattern attributeRegex = getAttributeRegex(context, flowFile);

        return attributeName -> attributeRegex.matcher(attributeName).matches();
    }

    /* properties */

    private static FilterMode getFilterMode(ProcessContext context, FlowFile flowFile) {
        final String rawFilterMode = context
                .getProperty(FILTER_MODE)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        if (FILTER_MODE_VALUE_REMOVE.getValue().equals(rawFilterMode)) {
            return FilterMode.REMOVE;
        }
        return FilterMode.RETAIN;
    }

    private static MatchingStrategy getMatchingStrategy(ProcessContext context) {
        final String rawMatchingStrategy = context
                .getProperty(MATCHING_STRATEGY)
                .getValue();

        if (MATCHING_STRATEGY_VALUE_REGEX.getValue().equals(rawMatchingStrategy)) {
            return MatchingStrategy.REGEX;
        }
        return MatchingStrategy.ENUMERATION;
    }

    private static String getAttributeSet(ProcessContext context, FlowFile flowFile) {
        return context.getProperty(ATTRIBUTE_SET).evaluateAttributeExpressions(flowFile).getValue();
    }

    private static String getDelimiter() {
        return DELIMITER_VALUE;
    }

    private static Pattern getAttributeRegex(ProcessContext context, FlowFile flowFile) {
        return Pattern.compile(
                context.getProperty(ATTRIBUTE_REGEX).evaluateAttributeExpressions(flowFile).getValue()
        );
    }

    private enum FilterMode {
        RETAIN,
        REMOVE,
    }

    private enum MatchingStrategy {
        ENUMERATION,
        REGEX,
    }
}
