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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generates a JSON representation of the input FlowFile Attributes. The resulting JSON " +
        "can be written to either a new Attribute 'JSONAttributes' or written to the FlowFile as content.")
@WritesAttribute(attribute = "JSONAttributes", description = "JSON representation of Attributes")
public class AttributesToJSON extends AbstractProcessor {

    public static final String JSON_ATTRIBUTE_NAME = "JSONAttributes";
    private static final String AT_LIST_SEPARATOR = ",";

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    private final String APPLICATION_JSON = "application/json";


    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON. If this value " +
                    "is left empty then all existing Attributes will be included. This list of attributes is " +
                    "case sensitive. If an attribute specified in the list is not found it will be be emitted " +
                    "to the resulting JSON with an empty string or NULL value.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Control if JSON value is written as a new flowfile attribute '" + JSON_ATTRIBUTE_NAME + "' " +
                    "or written in the flowfile content. Writing to flowfile content will overwrite any " +
                    "existing flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes which are " +
                    "contained in every FlowFile should be included in the final JSON value generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name(("If true a non existing or empty attribute will be NULL in the resulting JSON. If false an empty " +
                    "string will be placed in the JSON"))
            .description("")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to JSON").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to JSON").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private static final ObjectMapper objectMapper = new ObjectMapper();

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


    /**
     * Builds the Map of attributes that should be included in the JSON that is emitted from this process.
     *
     * @return
     *  Map of values that are feed to a Jackson ObjectMapper
     */
    protected Map<String, String> buildAttributesMapForFlowFile(FlowFile ff, String atrList,
                                                                boolean includeCoreAttributes,
                                                                boolean nullValForEmptyString) {

        Map<String, String> atsToWrite = new HashMap<>();

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, val);
                    } else {
                        if (nullValForEmptyString) {
                            atsToWrite.put(cleanStr, null);
                        } else {
                            atsToWrite.put(cleanStr, "");
                        }
                    }
                }
            }
        } else {
            atsToWrite.putAll(ff.getAttributes());
        }

        if (!includeCoreAttributes) {
            atsToWrite = removeCoreAttributes(atsToWrite);
        }

        return atsToWrite;
    }

    /**
     * Remove all of the CoreAttributes from the Attributes that will be written to the Flowfile.
     *
     * @param atsToWrite
     *  List of Attributes that have already been generated including the CoreAttributes
     *
     * @return
     *  Difference of all attributes minus the CoreAttributes
     */
    protected Map<String, String> removeCoreAttributes(Map<String, String> atsToWrite) {
        for (CoreAttributes c : CoreAttributes.values()) {
            atsToWrite.remove(c.key());
        }
        return atsToWrite;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final Map<String, String> atrList = buildAttributesMapForFlowFile(original,
                context.getProperty(ATTRIBUTES_LIST).getValue(),
                context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean(),
                context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean());

        try {

            switch (context.getProperty(DESTINATION).getValue()) {
                case DESTINATION_ATTRIBUTE:
                    FlowFile atFlowfile = session.putAttribute(original, JSON_ATTRIBUTE_NAME,
                            objectMapper.writeValueAsString(atrList));
                    session.transfer(atFlowfile, REL_SUCCESS);
                    break;
                case DESTINATION_CONTENT:
                    FlowFile conFlowfile = session.write(original, new StreamCallback() {
                        @Override
                        public void process(InputStream in, OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(objectMapper.writeValueAsBytes(atrList));
                            }
                        }
                    });
                    conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                    session.transfer(conFlowfile, REL_SUCCESS);
                    break;
            }

        } catch (JsonProcessingException e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
