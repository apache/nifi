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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.NLKBufferedReader;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"attributes", "routing", "text", "regexp", "regex", "Regular Expression", "Expression Language"})
@CapabilityDescription("Routes textual data based on a set of user-defined rules. Each line in an incoming FlowFile is compared against the values specified by user-defined Properties. "
    + "The mechanism by which the text is compared to these user-defined properties is defined by the 'Matching Strategy'. The data is then routed according to these rules, routing "
    + "each line of the text individually.")
@DynamicProperty(name = "Relationship Name", value = "value to match against", description = "Routes data that matches the value specified in the Dynamic Property Value to the "
    + "Relationship specified in the Dynamic Property Key.")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's value")
public class RouteText extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_KEY = "RouteText.Route";

    private static final String routeAllMatchValue = "Route to 'matched' if line matches all conditions";
    private static final String routeAnyMatchValue = "Route to 'matched' if lines matches any condition";
    private static final String routePropertyNameValue = "Route to each matching Property Name";

    private static final String startsWithValue = "Starts With";
    private static final String endsWithValue = "Ends With";
    private static final String containsValue = "Contains";
    private static final String equalsValue = "Equals";
    private static final String matchesRegularExpressionValue = "Matches Regular Expression";
    private static final String containsRegularExpressionValue = "Contains Regular Expression";


    public static final AllowableValue ROUTE_TO_MATCHING_PROPERTY_NAME = new AllowableValue(routePropertyNameValue, routePropertyNameValue,
        "Lines will be routed to each relationship whose corresponding expression evaluates to 'true'");
    public static final AllowableValue ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH = new AllowableValue(routeAllMatchValue, routeAllMatchValue,
        "Requires that all user-defined expressions evaluate to 'true' for the line to be considered a match");
    public static final AllowableValue ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES = new AllowableValue(routeAnyMatchValue, routeAnyMatchValue,
        "Requires that at least one user-defined expression evaluate to 'true' for the line to be considered a match");

    public static final AllowableValue STARTS_WITH = new AllowableValue(startsWithValue, startsWithValue,
        "Match lines based on whether the line starts with the property value");
    public static final AllowableValue ENDS_WITH = new AllowableValue(endsWithValue, endsWithValue,
        "Match lines based on whether the line ends with the property value");
    public static final AllowableValue CONTAINS = new AllowableValue(containsValue, containsValue,
        "Match lines based on whether the line contains the property value");
    public static final AllowableValue EQUALS = new AllowableValue(equalsValue, equalsValue,
        "Match lines based on whether the line equals the property value");
    public static final AllowableValue MATCHES_REGULAR_EXPRESSION = new AllowableValue(matchesRegularExpressionValue, matchesRegularExpressionValue,
        "Match lines based on whether the line exactly matches the Regular Expression that is provided as the Property value");
    public static final AllowableValue CONTAINS_REGULAR_EXPRESSION = new AllowableValue(containsRegularExpressionValue, containsRegularExpressionValue,
        "Match lines based on whether the line contains some text that matches the Regular Expression that is provided as the Property value");


    public static final PropertyDescriptor ROUTE_STRATEGY = new PropertyDescriptor.Builder()
        .name("Routing Strategy")
        .description("Specifies how to determine which Relationship(s) to use when evaluating the lines of incoming text against the 'Matching Strategy' and user-defined properties.")
        .required(true)
        .allowableValues(ROUTE_TO_MATCHING_PROPERTY_NAME, ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH, ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES)
        .defaultValue(ROUTE_TO_MATCHING_PROPERTY_NAME.getValue())
        .dynamic(false)
        .build();

    public static final PropertyDescriptor MATCH_STRATEGY = new PropertyDescriptor.Builder()
        .name("Matching Strategy")
        .description("Specifies how to evaluate each line of incoming text against the user-defined properties.")
        .required(true)
        .allowableValues(STARTS_WITH, ENDS_WITH, CONTAINS, EQUALS, MATCHES_REGULAR_EXPRESSION, CONTAINS_REGULAR_EXPRESSION)
        .dynamic(false)
        .build();

    public static final PropertyDescriptor TRIM_WHITESPACE = new PropertyDescriptor.Builder()
        .name("Ignore Leading/Trailing Whitespace")
        .description("Indicates whether or the whitespace at the beginning and end of the lines should be ignored when evaluating the line.")
        .required(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .dynamic(false)
        .build();

    static final PropertyDescriptor IGNORE_CASE = new PropertyDescriptor.Builder()
        .name("Ignore Case")
        .description("If true, capitalization will not be taken into account when comparing values. E.g., matching against 'HELLO' or 'hello' will have the same result.")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set in which the incoming text is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original input file will be routed to this destination when the lines have been successfully routed to 1 or more relationships")
        .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
        .name("unmatched")
        .description("Data that does not satisfy the required user-defined rules will be routed to this Relationship")
        .build();
    public static final Relationship REL_MATCH = new Relationship.Builder()
        .name("matched")
        .description("Data that satisfies the required user-defined rules will be routed to this Relationship")
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
        set.add(REL_ORIGINAL);
        set.add(REL_NO_MATCH);
        relationships = new AtomicReference<>(set);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ROUTE_STRATEGY);
        properties.add(MATCH_STRATEGY);
        properties.add(CHARACTER_SET);
        properties.add(TRIM_WHITESPACE);
        properties.add(IGNORE_CASE);
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
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamic(true)
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
            } else if (oldValue == null && descriptor.isDynamic()) { // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }

            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }

        // formulate the new set of Relationships
        final Set<String> allDynamicProps = this.dynamicPropertyNames;
        final Set<Relationship> newRelationships = new HashSet<>();
        final String routeStrategy = configuredRouteStrategy;
        if (ROUTE_TO_MATCHING_PROPERTY_NAME.equals(routeStrategy)) {
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
     *
     * @param context ProcessContext used to retrieve dynamic properties
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            getLogger().debug("Adding new dynamic property: {}", new Object[] {descriptor});
            newPropertyMap.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
        }

        this.propertyMap = newPropertyMap;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        boolean dynamicProperty = false;

        final String matchStrategy = validationContext.getProperty(MATCH_STRATEGY).getValue();
        final boolean compileRegex = matchStrategy.equals(matchesRegularExpressionValue) || matchStrategy.equals(containsRegularExpressionValue);
        Validator validator = null;
        if (compileRegex) {
            validator = StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true);
        }

        Map<PropertyDescriptor, String> allProperties = validationContext.getProperties();
        for (final PropertyDescriptor descriptor : allProperties.keySet()) {
            if (descriptor.isDynamic()) {
                dynamicProperty = true;

                if (compileRegex) {
                    ValidationResult validationResult = validator.validate(descriptor.getName(), validationContext.getProperty(descriptor).getValue(), validationContext);
                    if (validationResult != null) {
                        results.add(validationResult);
                    }
                }
            }
        }

        if (!dynamicProperty) {
            results.add(new ValidationResult.Builder().subject("Dynamic Properties")
                .explanation("In order to route text there must be dynamic properties to match against").valid(false).build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final boolean trim = context.getProperty(TRIM_WHITESPACE).asBoolean();
        final String routeStrategy = context.getProperty(ROUTE_STRATEGY).getValue();
        final String matchStrategy = context.getProperty(MATCH_STRATEGY).getValue();
        final boolean ignoreCase = context.getProperty(IGNORE_CASE).asBoolean();

        final Map<Relationship, PropertyValue> propMap = this.propertyMap;
        final Map<Relationship, Object> propValueMap = new HashMap<>(propMap.size());

        final boolean compileRegex = matchStrategy.equals(matchesRegularExpressionValue) || matchStrategy.equals(containsRegularExpressionValue);

        for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {
            final String value = entry.getValue().evaluateAttributeExpressions(originalFlowFile).getValue();

            Pattern compiledRegex = null;
            if (compileRegex) {
                compiledRegex = ignoreCase ? Pattern.compile(value, Pattern.CASE_INSENSITIVE) : Pattern.compile(value);
            }
            propValueMap.put(entry.getKey(), compileRegex ? compiledRegex : value);
        }

        final Map<Relationship, FlowFile> flowFileMap = new HashMap<>();

        session.read(originalFlowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                try (final Reader inReader = new InputStreamReader(in,charset);
                    final NLKBufferedReader reader = new NLKBufferedReader(inReader)) {

                    String line;
                    while ((line = reader.readLine()) != null) {

                        int propertiesThatMatchedLine = 0;
                        for (final Map.Entry<Relationship, Object> entry : propValueMap.entrySet()) {

                            String matchLine = trim ? line.trim() : line;
                            boolean lineMatchesProperty = lineMatches(matchLine, entry.getValue(), context.getProperty(MATCH_STRATEGY).getValue(), ignoreCase);
                            if (lineMatchesProperty) {
                                propertiesThatMatchedLine++;
                            }

                            if (lineMatchesProperty && ROUTE_TO_MATCHING_PROPERTY_NAME.getValue().equals(routeStrategy)) {
                                // route each individual line to each Relationship that matches. This one matches.
                                final Relationship relationship = entry.getKey();
                                appendLine(session, flowFileMap, relationship, originalFlowFile, line, charset);
                                continue;
                            }

                            // break as soon as possible to avoid calculating things we don't need to calculate.
                            if (lineMatchesProperty && ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES.getValue().equals(routeStrategy)) {
                                break;
                            }

                            if (!lineMatchesProperty && ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH.getValue().equals(routeStrategy)) {
                                break;
                            }
                        }

                        final Relationship relationship;
                        if (ROUTE_TO_MATCHING_PROPERTY_NAME.getValue().equals(routeStrategy) && propertiesThatMatchedLine > 0) {
                            // Set relationship to null so that we do not append the line to each FlowFile again. #appendLine is called
                            // above within the loop, as the line may need to go to multiple different FlowFiles.
                            relationship = null;
                        } else if (ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES.getValue().equals(routeStrategy) && propertiesThatMatchedLine > 0) {
                            relationship = REL_MATCH;
                        } else if (ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH.getValue().equals(routeStrategy) && propertiesThatMatchedLine == propValueMap.size()) {
                            relationship = REL_MATCH;
                        } else {
                            relationship = REL_NO_MATCH;
                        }

                        if (relationship != null) {
                            appendLine(session, flowFileMap, relationship, originalFlowFile, line, charset);
                        }
                    }
                }
            }
        });

        for (final Map.Entry<Relationship, FlowFile> entry : flowFileMap.entrySet()) {
            logger.info("Created {} from {}; routing to relationship {}", new Object[] {entry.getValue(), originalFlowFile, entry.getKey()});
            FlowFile updatedFlowFile = session.putAttribute(entry.getValue(), ROUTE_ATTRIBUTE_KEY, entry.getKey().getName());
            session.getProvenanceReporter().route(updatedFlowFile, entry.getKey());
            session.transfer(updatedFlowFile, entry.getKey());
        }

        // now transfer the original flow file
        FlowFile flowFile = originalFlowFile;
        logger.info("Routing {} to {}", new Object[] {flowFile, REL_ORIGINAL});
        session.getProvenanceReporter().route(originalFlowFile, REL_ORIGINAL);
        flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, REL_ORIGINAL.getName());
        session.transfer(flowFile, REL_ORIGINAL);
    }


    private void appendLine(final ProcessSession session, final Map<Relationship, FlowFile> flowFileMap,
        final Relationship relationship, final FlowFile original, final String line, final Charset charset) {
        FlowFile flowFile = flowFileMap.get(relationship);
        if (flowFile == null) {
            flowFile = session.create(original);
        }

        flowFile = session.append(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(line.getBytes(charset));
            }
        });

        flowFileMap.put(relationship, flowFile);
    }


    protected static boolean lineMatches(final String line, final Object comparison, final String matchingStrategy, final boolean ignoreCase) {
        switch (matchingStrategy) {
            case startsWithValue:
                return line.toLowerCase().startsWith(((String) comparison).toLowerCase());
            case endsWithValue:
                return line.toLowerCase().endsWith(((String) comparison).toLowerCase());
            case containsValue:
                return line.toLowerCase().contains(((String) comparison).toLowerCase());
            case equalsValue:
                return line.equalsIgnoreCase((String) comparison);
            case matchesRegularExpressionValue:
                return ((Pattern) comparison).matcher(line).matches();
            case containsRegularExpressionValue:
                return ((Pattern) comparison).matcher(line).find();
        }

        return false;
    }
}