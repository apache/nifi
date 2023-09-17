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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@Tags({"modify", "html", "dom", "css", "element"})
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Modifies the value of an existing HTML element. The desired element to be modified is located by" +
        " using CSS selector syntax. The incoming HTML is first converted into a HTML Document Object Model so that HTML elements may be selected" +
        " in the similar manner that CSS selectors are used to apply styles to HTML. The resulting HTML DOM is then \"queried\"" +
        " using the user defined CSS selector string to find the element the user desires to modify. If the HTML element is found" +
        " the element's value is updated in the DOM using the value specified \"Modified Value\" property. All DOM elements" +
        " that match the CSS selector will be updated. Once all of the DOM elements have been updated the DOM is rendered" +
        " to HTML and the result replaces the flowfile content with the updated HTML. A more thorough reference for the" +
        " CSS selector syntax can be found at" +
        " \"http://jsoup.org/apidocs/org/jsoup/select/Selector.html\"")
@SeeAlso({GetHTMLElement.class, PutHTMLElement.class})
@WritesAttributes({@WritesAttribute(attribute="NumElementsModified", description="Total number of HTML " +
        "element modifications made")})
public class ModifyHTMLElement extends AbstractHTMLProcessor {

    public static final String NUM_ELEMENTS_MODIFIED_ATTR = "NumElementsModified";

    public static final PropertyDescriptor OUTPUT_TYPE = new PropertyDescriptor.Builder()
            .name("Output Type")
            .description("Controls whether the HTML element is output as " +
                    ELEMENT_HTML + "," + ELEMENT_TEXT + " or " + ELEMENT_DATA)
            .required(true)
            .allowableValues(ELEMENT_HTML, ELEMENT_TEXT, ELEMENT_ATTRIBUTE)
            .defaultValue(ELEMENT_HTML)
            .build();

    public static final PropertyDescriptor MODIFIED_VALUE = new PropertyDescriptor
            .Builder().name("Modified Value")
            .description("Value to update the found HTML elements with")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_KEY = new PropertyDescriptor
            .Builder().name("Attribute Name")
            .description(("When modifying the value of an element attribute this value is used as the key to determine" +
                    " which attribute on the selected element will be modified with the new value."))
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CSS_SELECTOR);
        descriptors.add(HTML_CHARSET);
        descriptors.add(OUTPUT_TYPE);
        descriptors.add(MODIFIED_VALUE);
        descriptors.add(ATTRIBUTE_KEY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_INVALID_HTML);
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

    /**
     * This processor used to support URL property, but it has been removed
     * since it's not required when altering HTML elements.
     * Support URL as dynamic property so that existing data flow can stay in valid state without modification.
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return URL;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Document doc;
        final Elements eles;
        try {
            doc = parseHTMLDocumentFromFlowfile(flowFile, context, session);
            eles = doc.select(context.getProperty(CSS_SELECTOR).evaluateAttributeExpressions(flowFile).getValue());
        } catch (Exception ex) {
            getLogger().error("Failed to extract HTML from {} due to {}; routing to {}", flowFile, ex.toString(), REL_INVALID_HTML.getName(), ex);
            session.transfer(flowFile, REL_INVALID_HTML);
            return;
        }

        final String modifiedValue = context.getProperty(MODIFIED_VALUE).evaluateAttributeExpressions(flowFile).getValue();

        if (eles == null || eles.size() == 0) {
            // No element found
            session.transfer(flowFile, REL_NOT_FOUND);
        } else {
            for (Element ele : eles) {
                switch (context.getProperty(OUTPUT_TYPE).getValue()) {
                    case ELEMENT_HTML:
                        ele.html(modifiedValue);
                        break;
                    case ELEMENT_ATTRIBUTE:
                        ele.attr(context.getProperty(ATTRIBUTE_KEY).evaluateAttributeExpressions(flowFile).getValue(), modifiedValue);
                        break;
                    case ELEMENT_TEXT:
                        ele.text(modifiedValue);
                        break;
                }
            }

            FlowFile ff = session.write(session.create(flowFile), new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    out.write(doc.html().getBytes(StandardCharsets.UTF_8));
                }
            });
            ff = session.putAttribute(ff, NUM_ELEMENTS_MODIFIED_ATTR, new Integer(eles.size()).toString());
            session.transfer(ff, REL_SUCCESS);

            // Transfer the original HTML
            session.transfer(flowFile, REL_ORIGINAL);
        }
    }

}
