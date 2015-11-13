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
package org.apache.nifi;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@Tags({"get", "html", "dom", "css", "element"})
@CapabilityDescription("Parses HTML input using CSS selector syntax and creates a new flowfile containing the extracted" +
        " element content for each matching CSS selector.")
@SeeAlso({ModifyHTMLElement.class, PutHTMLElement.class})
@WritesAttributes({@WritesAttribute(attribute="HTMLElement", description="Flowfile attribute where the element result" +
        " parsed from the HTML using the CSS selector syntax are placed if the destination is a flowfile attribute.")})
public class GetHTMLElement
        extends AbstractHTMLProcessor {

    public static final String HTML_ELEMENT_ATTRIBUTE_NAME = "HTMLElement";
    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final PropertyDescriptor PREPEND_ELEMENT_VALUE = new PropertyDescriptor
            .Builder().name("Prepend Element value")
            .description("Prepends the specified value to the resulting Element")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor APPEND_ELEMENT_VALUE = new PropertyDescriptor
            .Builder().name("Append Element value")
            .description("Appends the specified value to the resulting Element")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_KEY = new PropertyDescriptor
            .Builder().name("Attribute Name")
            .description(("When getting the value of an element attribute this value is used as the key to determine" +
                    " which attribute on the selected element should be retrieved."))
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();


    public static final PropertyDescriptor OUTPUT_TYPE = new PropertyDescriptor.Builder()
            .name("Output Type")
            .description("Controls the type of value that is retrieved from the element. " +
                    ELEMENT_HTML + "," + ELEMENT_TEXT + ", " + ELEMENT_ATTRIBUTE + " or " + ELEMENT_DATA)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(ELEMENT_HTML, ELEMENT_TEXT, ELEMENT_ATTRIBUTE, ELEMENT_DATA)
            .defaultValue(ELEMENT_HTML)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Control if element extracted is written as a flowfile attribute or " +
                    "as flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(URL);
        descriptors.add(CSS_SELECTOR);
        descriptors.add(HTML_CHARSET);
        descriptors.add(OUTPUT_TYPE);
        descriptors.add(DESTINATION);
        descriptors.add(PREPEND_ELEMENT_VALUE);
        descriptors.add(APPEND_ELEMENT_VALUE);
        descriptors.add(ATTRIBUTE_KEY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_NOT_FOUND);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {

            final Document doc = parseHTMLDocumentFromFlowfile(flowFile, context, session);
            final Elements eles = doc.select(context.getProperty(CSS_SELECTOR)
                    .evaluateAttributeExpressions().getValue());
            final String prependValue = context.getProperty(PREPEND_ELEMENT_VALUE)
                    .evaluateAttributeExpressions(flowFile).getValue();
            final String appendValue = context.getProperty(APPEND_ELEMENT_VALUE)
                    .evaluateAttributeExpressions(flowFile).getValue();

            if (eles == null || eles.size() == 0) {
                //No element found
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                for (final Element ele : eles) {
                    final FlowFile ff = session.create();

                    switch (context.getProperty(DESTINATION).getValue()) {
                        case DESTINATION_ATTRIBUTE:
                            final FlowFile atFlowfile = session.putAttribute(ff, HTML_ELEMENT_ATTRIBUTE_NAME,
                                    extractElementValue(
                                            prependValue,
                                            context.getProperty(OUTPUT_TYPE).getValue(),
                                            appendValue,
                                            ele,
                                            context.getProperty(ATTRIBUTE_KEY).evaluateAttributeExpressions()
                                                    .getValue()));
                            session.getProvenanceReporter().create(atFlowfile);
                            session.transfer(atFlowfile, REL_SUCCESS);
                            break;
                        case DESTINATION_CONTENT:
                            final FlowFile conFlowfile = session.write(ff, new StreamCallback() {
                                @Override
                                public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                                    try {
                                        outputStream.write(extractElementValue(
                                                prependValue,
                                                context.getProperty(OUTPUT_TYPE).getValue(),
                                                appendValue,
                                                ele,
                                                context.getProperty(ATTRIBUTE_KEY).evaluateAttributeExpressions()
                                                        .getValue()).getBytes());
                                    } catch (Exception ex) {
                                        session.transfer(ff, REL_FAILURE);
                                    }
                                }
                            });

                            session.getProvenanceReporter().create(conFlowfile);
                            session.transfer(conFlowfile, REL_SUCCESS);
                            break;
                    }

                }

                //Transfer the original HTML
                session.transfer(flowFile, REL_ORIGINAL);
            }

        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }

    }


    /**
     * Extracts the HTML value based on the configuration values.
     *
     * @return
     *  value from the parsed HTML element
     */
    private String extractElementValue(String prependValue, String outputType, String appendValue, Element ele,
                                       String attrKey) {
        if (StringUtils.isEmpty(prependValue)) {
            prependValue = "";
        }
        if (StringUtils.isEmpty(appendValue)) {
            appendValue = "";
        }

        switch (outputType) {
            case ELEMENT_HTML:
                return prependValue + ele.html() + appendValue;
            case ELEMENT_TEXT:
                return prependValue + ele.text() + appendValue;
            case ELEMENT_DATA:
                return prependValue + ele.data() + appendValue;
            case ELEMENT_ATTRIBUTE:
                return prependValue + ele.attr(attrKey) + appendValue;
            default:
                return prependValue + ele.html() + appendValue;
        }
    }

}
