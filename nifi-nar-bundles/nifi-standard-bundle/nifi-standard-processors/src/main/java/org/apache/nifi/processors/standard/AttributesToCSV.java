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

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Collections;
import java.util.stream.Collectors;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"csv", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generates a CSV representation of the input FlowFile Attributes. The resulting CSV " +
        "can be written to either a newly generated attribute named 'CSVAttributes' or written to the FlowFile as content.  " +
        "If the attribute value contains a comma, newline or double quote, then the attribute value will be " +
        "escaped with double quotes.  Any double quote characters in the attribute value are escaped with " +
        "another double quote.  If the attribute value does not contain a comma, newline or double quote, then the " +
        "attribute value is returned unchanged.")
@WritesAttribute(attribute = "CSVAttributes", description = "CSV representation of Attributes")
public class AttributesToCSV extends AbstractProcessor {

    private static final String OUTPUT_NEW_ATTRIBUTE = "flowfile-attribute";
    private static final String OUTPUT_OVERWRITE_CONTENT = "flowfile-content";
    private static final String OUTPUT_ATTRIBUTE_NAME = "CSVAttributes";
    private static final String OUTPUT_SEPARATOR = ",";
    private static final String OUTPUT_MIME_TYPE = "text/csv";


    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("attribute-list")
            .displayName("Attribute List")
            .description("Comma separated list of attributes to be included in the resulting CSV. If this value " +
                    "is left empty then all existing Attributes will be included. This list of attributes is " +
                    "case sensitive and does not support attribute names that contain commas. If an attribute specified in the list is not found it will be emitted " +
                    "to the resulting CSV with an empty string or null depending on the 'Null Value' property. " +
                    "If a core attribute is specified in this list " +
                    "and the 'Include Core Attributes' property is false, the core attribute will be included. The attribute list " +
                    "ALWAYS wins.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("destination")
            .displayName("Destination")
            .description("Control if CSV value is written as a new flowfile attribute 'CSVAttributes' " +
                    "or written in the flowfile content. Writing to flowfile content will overwrite any " +
                    "existing flowfile content.")
            .required(true)
            .allowableValues(OUTPUT_NEW_ATTRIBUTE, OUTPUT_OVERWRITE_CONTENT)
            .defaultValue(OUTPUT_NEW_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("include-core-userSpecifiedAttributes")
            .displayName("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes, which are " +
                    "contained in every FlowFile, should be included in the final CSV value generated.  The Attribute List property " +
                    "overrides this setting.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name(("null-value"))
            .displayName("Null Value")
            .description("If true a non existing or empty attribute will be 'null' in the resulting CSV. If false an empty " +
                    "string will be placed in the CSV")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to CSV").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to CSV").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private volatile Set<String> userSpecifiedAttributes;
    private volatile Boolean includeCoreAttributes;
    private volatile Set<String> coreAttributes;
    private volatile boolean destinationContent;
    private volatile boolean nullValForEmptyString;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTES_LIST);
        properties.add(DESTINATION);
        properties.add(INCLUDE_CORE_ATTRIBUTES);
        properties.add(NULL_VALUE_FOR_EMPTY_STRING);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    private Map<String, String> buildAttributesMapForFlowFile(FlowFile ff, Set<String> userSpecifiedAttributes) {
        Map<String, String> result;
        if (!userSpecifiedAttributes.isEmpty()) {
            //the user gave a list of attributes
            result = new HashMap<>(userSpecifiedAttributes.size());
            for (String attribute : userSpecifiedAttributes) {
                String val = ff.getAttribute(attribute);
                if (val != null && !val.isEmpty()) {
                    result.put(attribute, val);
                } else {
                    if (nullValForEmptyString) {
                        result.put(attribute, "null");
                    } else {
                        result.put(attribute, "");
                    }
                }
            }
        } else {
            //the user did not give a list of attributes, take all the attributes from the flowfile
            Map<String, String> ffAttributes = ff.getAttributes();
            result = new HashMap<>(ffAttributes.size());
            result.putAll(ffAttributes);
        }

        //now glue on the core attributes if the user wants them.
        if(includeCoreAttributes) {
            for (String coreAttribute : coreAttributes) {
                //make sure this coreAttribute is applicable to this flowfile.
                String val = ff.getAttribute(coreAttribute);
                if(ff.getAttributes().containsKey(coreAttribute)) {
                    if (!StringUtils.isEmpty(val)){
                        result.put(coreAttribute, val);
                    } else {
                        if (nullValForEmptyString) {
                            result.put(coreAttribute, "null");
                        } else {
                            result.put(coreAttribute, "");
                        }
                    }
                }
            }
        } else {
            //remove core attributes since the user does not want them, unless they are in the attribute list. Attribute List always wins
            for (String coreAttribute : coreAttributes) {
                //never override user specified attributes, even if the user has selected to exclude core attributes
                if(!userSpecifiedAttributes.contains(coreAttribute)) {
                    result.remove(coreAttribute);
                }
            }
        }
        return result;
    }

    private Set<String> attributeListStringToSet(String attributeList) {
        //take the user specified attribute list string and convert to list of strings.
        Set<String> result = new HashSet<>();
        if (StringUtils.isNotBlank(attributeList)) {
            String[] ats = StringUtils.split(attributeList, OUTPUT_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String trim = str.trim();
                        result.add(trim);
                }
            }
        }
        return result;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        includeCoreAttributes = context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean();
        coreAttributes = Arrays.stream(CoreAttributes.values()).map(CoreAttributes::key).collect(Collectors.toSet());
        destinationContent = OUTPUT_OVERWRITE_CONTENT.equals(context.getProperty(DESTINATION).getValue());
        nullValForEmptyString = context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean();
     }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        userSpecifiedAttributes = attributeListStringToSet(context.getProperty(ATTRIBUTES_LIST).evaluateAttributeExpressions(original).getValue());

        final Map<String, String> atrList = buildAttributesMapForFlowFile(original, userSpecifiedAttributes);

        //escape attribute values
        int index = 0;
        final int atrListSize = atrList.values().size() -1;
        final StringBuilder sb = new StringBuilder();
        for (final String val : atrList.values()) {
            sb.append(StringEscapeUtils.escapeCsv(val));
            sb.append(index++ < atrListSize ? OUTPUT_SEPARATOR : "");
        }

        try {
            if (destinationContent) {
                FlowFile conFlowfile = session.write(original, (in, out) -> {
                        out.write(sb.toString().getBytes());
                });
                conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), OUTPUT_MIME_TYPE);
                session.transfer(conFlowfile, REL_SUCCESS);
            } else {
                FlowFile atFlowfile = session.putAttribute(original, OUTPUT_ATTRIBUTE_NAME , sb.toString());
                session.transfer(atFlowfile, REL_SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
