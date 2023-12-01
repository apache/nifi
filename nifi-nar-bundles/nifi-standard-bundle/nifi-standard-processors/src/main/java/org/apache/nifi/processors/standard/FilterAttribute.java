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
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

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
@CapabilityDescription("Filters the attributes of a FlowFile by retaining specified attributes and removing the rest or by removing specified attributes and retaining the rest.")
public class FilterAttribute extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successful FlowFiles are routed to this relationship")
            .build();

    public static final PropertyDescriptor FILTER_MODE = new PropertyDescriptor.Builder()
            .name("Filter Mode")
            .displayName("Filter Mode")
            .description("Specifies the strategy to apply on filtered attributes. Either 'Remove' or 'Retain' only the matching attributes.")
            .required(true)
            .allowableValues(FilterMode.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue(FilterMode.RETAIN.getValue())
            .build();

    public static final PropertyDescriptor MATCHING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Attribute Matching Strategy")
            .displayName("Attribute Matching Strategy")
            .description("Specifies the strategy to filter attributes by.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(MatchingStrategy.class)
            .defaultValue(MatchingStrategy.ENUMERATION.getValue())
            .build();

    public static final PropertyDescriptor ATTRIBUTE_ENUMERATION = new PropertyDescriptor.Builder()
            .name("Filtered Attributes")
            .displayName("Filtered Attributes")
            .description("A set of attribute names to filter from FlowFiles. Each attribute name is separated by the comma delimiter ','.")
            .required(true)
            .dependsOn(MATCHING_STRATEGY, MatchingStrategy.ENUMERATION.getValue())
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_PATTERN = new PropertyDescriptor.Builder()
            .name("Filtered Attributes Pattern")
            .displayName("Filtered Attributes Pattern")
            .description("A regular expression to match names of attributes to filter from FlowFiles.")
            .required(true)
            .dependsOn(MATCHING_STRATEGY, MatchingStrategy.PATTERN.getValue())
            .addValidator(StandardValidators.REGULAR_EXPRESSION_WITH_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private final static String DELIMITER_VALUE = ",";

    private final static Set<Relationship> relationships = Collections.singleton(REL_SUCCESS);
    private final static List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(FILTER_MODE, MATCHING_STRATEGY, ATTRIBUTE_ENUMERATION, ATTRIBUTE_PATTERN)
    );

    private volatile Predicate<String> cachedMatchingPredicate;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final MatchingStrategy matchingStrategy = getMatchingStrategy(context);

        cachedMatchingPredicate = null;

        if (matchingStrategy == MatchingStrategy.ENUMERATION
                && !context.getProperty(ATTRIBUTE_ENUMERATION).isExpressionLanguagePresent()) {
            cachedMatchingPredicate = determineMatchingPredicateBasedOnEnumeration(context, null);
        }
        if (matchingStrategy == MatchingStrategy.PATTERN
                && !context.getProperty(ATTRIBUTE_PATTERN).isExpressionLanguagePresent()) {
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

        final FilterMode filterMode = getFilterMode(context);
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
            case PATTERN:
                return determineMatchingPredicateBasedOnRegex(context, flowFile);
            default:
                throw new IllegalArgumentException(
                        "Cannot determine matching predicate for unsupported strategy [" + matchingStrategy + "]"
                );
        }
    }

    private static Predicate<String> determineMatchingPredicateBasedOnEnumeration(ProcessContext context, FlowFile flowFile) {
        final String attributeSetDeclaration = getAttributeSet(context, flowFile);
        final String delimiter = getDelimiter();

        Set<String> attributeSet = Arrays.stream(attributeSetDeclaration.split(Pattern.quote(delimiter)))
                .map(String::trim)
                .filter(attributeName -> !attributeName.isEmpty())
                .collect(Collectors.toSet());

        return attributeSet::contains;
    }

    private static Predicate<String> determineMatchingPredicateBasedOnRegex(ProcessContext context, FlowFile flowFile) {
        Pattern attributeRegex = getAttributeRegex(context, flowFile);

        return attributeName -> attributeRegex.matcher(attributeName).matches();
    }

    /* properties */

    private static FilterMode getFilterMode(ProcessContext context) {
        return FilterMode.fromValue(
                context.getProperty(FILTER_MODE).getValue()
        );
    }

    private static MatchingStrategy getMatchingStrategy(ProcessContext context) {
        return MatchingStrategy.fromValue(
                context.getProperty(MATCHING_STRATEGY).getValue()
        );
    }

    private static String getAttributeSet(ProcessContext context, FlowFile flowFile) {
        return context.getProperty(ATTRIBUTE_ENUMERATION).evaluateAttributeExpressions(flowFile).getValue();
    }

    private static String getDelimiter() {
        return DELIMITER_VALUE;
    }

    private static Pattern getAttributeRegex(ProcessContext context, FlowFile flowFile) {
        return Pattern.compile(
                context.getProperty(ATTRIBUTE_PATTERN).evaluateAttributeExpressions(flowFile).getValue()
        );
    }

    enum FilterMode implements DescribedValue {
        RETAIN(
                "Retain",
                "Retains only the attributes matching the filter, all other attributes are removed."
        ),
        REMOVE(
                "Remove",
                "Removes the attributes matching the filter, all other attributes are retained."
        );

        private final String value;
        private final String description;

        FilterMode(final String value, final String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String getValue() {
            return this.value;
        }

        @Override
        public String getDisplayName() {
            return this.value;
        }

        @Override
        public String getDescription() {
            return this.description;
        }

        public static FilterMode fromValue(final String value) {
            return FilterAttribute.enumConstantFromValue(FilterMode.class, value);
        }
    }

    enum MatchingStrategy implements DescribedValue {
        ENUMERATION(
                "Enumerate attributes",
                "Provides a set of attribute keys to filter for, separated by a comma delimiter ','."
        ),
        PATTERN(
                "Use regular expression",
                "Provides a regular expression to match keys of attributes to filter for."
        );

        private final String value;
        private final String description;

        MatchingStrategy(final String value, final String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String getValue() {
            return this.value;
        }

        @Override
        public String getDisplayName() {
            return this.value;
        }

        @Override
        public String getDescription() {
            return this.description;
        }

        public static MatchingStrategy fromValue(final String value) {
            return FilterAttribute.enumConstantFromValue(MatchingStrategy.class, value);
        }
    }

    private static <E extends Enum<E> & DescribedValue> E enumConstantFromValue(final Class<E> enumClass, final String value) {
        for (E enumConstant : enumClass.getEnumConstants()) {
            if (enumConstant.getValue().equals(value)) {
                return enumConstant;
            }
        }

        throw new IllegalArgumentException(String.format("Unknown %s value [%s]", enumClass.getSimpleName(), value));
    }
}