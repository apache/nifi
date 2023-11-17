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
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "filter", "retain", "delete", "Attribute Expression Language"})
@CapabilityDescription("Filters the Attributes for a FlowFile against a set of attribute names to retain or remove")
@UseCase(
        description = "Retain only a specified set of FlowFile attributes",
        configuration = """
        Define a "Delimiter" that does not occur in the names of any of the attributes to retain.
        Specify the set of "Attributes to filter" using the delimiter defined before.
        Set "Filter mode" to "Retain".
        """
)
public class FilterAttributes extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All successful FlowFiles are routed to this relationship").name("success").build();

    private final static Set<Relationship> relationships = Collections.singleton(REL_SUCCESS);

    public static final PropertyDescriptor ATTRIBUTE_SET = new PropertyDescriptor.Builder()
            .name("ATTRIBUTE_SET")
            .displayName("Attributes to filter")
            .description("A set of attribute names to filter from FlowFiles. Each attribute name is separated by the delimiter.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
            .name("DELIMITER")
            .displayName("Delimiter")
            .description("One or multiple characters that separates one attribute name value from another.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(",")
            .build();

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

    private final static List<PropertyDescriptor> properties = List.of(ATTRIBUTE_SET, DELIMITER, FILTER_MODE);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private Set<String> preCalculatedAttributes;


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (context.getProperty(ATTRIBUTE_SET).isExpressionLanguagePresent()
                || context.getProperty(DELIMITER).isExpressionLanguagePresent()) {
            // the attribute set may rely on FlowFile attributes; thus we cannot pre-calculate them
            preCalculatedAttributes = null;
        } else {
            preCalculatedAttributes = calculateAttributeSet(context, null);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Set<String> declaredAttributes;
        if (preCalculatedAttributes != null) {
            declaredAttributes = preCalculatedAttributes;
        } else {
            declaredAttributes = calculateAttributeSet(context, flowFile);
        }

        final FilterMode filterMode = getFilterMode(context, flowFile);
        final Set<String> attributesToRemove = computeAttributesToRemove(flowFile, filterMode, declaredAttributes);

        session.removeAllAttributes(flowFile, attributesToRemove);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static Set<String> calculateAttributeSet(ProcessContext context, FlowFile flowFile) {
        final String attributeSet = getAttributeSet(context, flowFile);
        final String delimiter = getDelimiter(context, flowFile);

        return parseAttributeSet(attributeSet, delimiter);
    }

    private static String getAttributeSet(ProcessContext context, FlowFile flowFile) {
        PropertyValue attributeSetProperty = context.getProperty(ATTRIBUTE_SET);

        if (flowFile != null) {
            attributeSetProperty = attributeSetProperty.evaluateAttributeExpressions(flowFile);
        }

        return attributeSetProperty.getValue();
    }

    private static String getDelimiter(ProcessContext context, FlowFile flowFile) {
        PropertyValue delimiterProperty = context.getProperty(DELIMITER);

        if (flowFile != null) {
            delimiterProperty = delimiterProperty.evaluateAttributeExpressions(flowFile);
        }

        return delimiterProperty.getValue();
    }

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

    private static Set<String> parseAttributeSet(final String attributeSetDeclaration, final String delimiter) {
        return Arrays.stream(attributeSetDeclaration.split(Pattern.quote(delimiter)))
                .map(String::trim)
                .filter(attributeName -> !attributeName.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static Set<String> computeAttributesToRemove(FlowFile flowFile, FilterMode filterMode, Set<String> declaredAttributes) {
        if (filterMode == FilterMode.REMOVE) {
            return declaredAttributes;
        }

        final Set<String> attributesToRemove = new HashSet<>(flowFile.getAttributes().keySet());
        attributesToRemove.removeAll(declaredAttributes);

        return attributesToRemove;
    }

    private enum FilterMode {
        RETAIN,
        REMOVE,
    }
}
