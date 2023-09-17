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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@SupportsBatching
@Tags({"get", "html", "dom", "css", "element"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Extracts HTML element values from the incoming flowfile's content using a CSS selector." +
        " The incoming HTML is first converted into a HTML Document Object Model so that HTML elements may be selected" +
        " in the similar manner that CSS selectors are used to apply styles to HTML. The resulting HTML DOM is then \"queried\"" +
        " using the user defined CSS selector string. The result of \"querying\" the HTML DOM may produce 0-N results." +
        " If no results are found the flowfile will be transferred to the \"element not found\" relationship to indicate" +
        " so to the end user. If N results are found a new flowfile will be created and emitted for each result. The query result will" +
        " either be placed in the content of the new flowfile or as an attribute of the new flowfile. By default the result is written to an" +
        " attribute. This can be controlled by the \"Destination\" property. Resulting query values may also have data" +
        " prepended or appended to them by setting the value of property \"Prepend Element Value\" or \"Append Element Value\"." +
        " Prepended and appended values are treated as string values and concatenated to the result retrieved from the" +
        " HTML DOM query operation. A more thorough reference for the CSS selector syntax can be found at" +
        " \"http://jsoup.org/apidocs/org/jsoup/select/Selector.html\"")
@SeeAlso({ModifyHTMLElement.class, PutHTMLElement.class})
@WritesAttributes({@WritesAttribute(attribute="HTMLElement", description="Flowfile attribute where the element result" +
        " parsed from the HTML using the CSS selector syntax are placed if the destination is a flowfile attribute.")})
public class GetHTMLElement
        extends AbstractHTMLProcessor {

    public static final String HTML_ELEMENT_ATTRIBUTE_NAME = "HTMLElement";
    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final PropertyDescriptor PREPEND_ELEMENT_VALUE = new PropertyDescriptor
            .Builder().name("Prepend Element Value")
            .description("Prepends the specified value to the resulting Element")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor APPEND_ELEMENT_VALUE = new PropertyDescriptor
            .Builder().name("Append Element Value")
            .description("Appends the specified value to the resulting Element")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_KEY = new PropertyDescriptor
            .Builder().name("Attribute Name")
            .description(("When getting the value of a HTML element attribute this value is used as the key to determine" +
                    " which attribute on the selected element should be retrieved. This value is used when the \"Output Type\"" +
                    " is set to \"" + ELEMENT_ATTRIBUTE + "\"." +
                    " If this value is prefixed with 'abs:', then the extracted attribute value will be converted into" +
                    " an absolute URL form using the specified base URL."))
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUTPUT_TYPE = new PropertyDescriptor.Builder()
            .name("Output Type")
            .description("Controls the type of DOM value that is retrieved from the HTML element.")
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final Document doc;
        final Elements eles;

        try {
            doc = parseHTMLDocumentFromFlowfile(flowFile, context, session);
            eles = doc.select(context.getProperty(CSS_SELECTOR).evaluateAttributeExpressions(flowFile).getValue());
        } catch (final Exception ex) {
            getLogger().error("Failed to extract HTML from {} due to {}; routing to {}", flowFile, ex, REL_INVALID_HTML, ex);
            session.transfer(flowFile, REL_INVALID_HTML);
            return;
        }

        final String prependValue = context.getProperty(PREPEND_ELEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String appendValue = context.getProperty(APPEND_ELEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String outputType = context.getProperty(OUTPUT_TYPE).getValue();
        final String attributeKey = context.getProperty(ATTRIBUTE_KEY).evaluateAttributeExpressions(flowFile).getValue();

        if (eles == null || eles.isEmpty()) {
            // No element found
            session.transfer(flowFile, REL_NOT_FOUND);
        } else {
            // Create a new FlowFile for each matching element.
            for (final Element ele : eles) {
                final String extractedElementValue = extractElementValue(prependValue, outputType, appendValue, ele, attributeKey);

                final FlowFile ff = session.create(flowFile);
                FlowFile updatedFF = ff;

                switch (context.getProperty(DESTINATION).getValue()) {
                    case DESTINATION_ATTRIBUTE:
                        updatedFF = session.putAttribute(ff, HTML_ELEMENT_ATTRIBUTE_NAME, extractedElementValue);
                        break;
                    case DESTINATION_CONTENT:
                        updatedFF = session.write(ff, new StreamCallback() {
                            @Override
                            public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
                                outputStream.write(extractedElementValue.getBytes(StandardCharsets.UTF_8));
                            }
                        });

                        break;
                }

                session.transfer(updatedFF, REL_SUCCESS);
            }

            // Transfer the original HTML
            session.transfer(flowFile, REL_ORIGINAL);
        }
    }


    /**
     * Extracts the HTML value based on the configuration values.
     *
     * @return value from the parsed HTML element
     */
    private String extractElementValue(String prependValue, final String outputType, String appendValue, final Element ele, final String attrKey) {
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

    @Override
    protected String getBaseUrl(FlowFile inputFlowFile, ProcessContext context) {
        return context.getProperty(URL).evaluateAttributeExpressions(inputFlowFile).getValue();
    }
}
