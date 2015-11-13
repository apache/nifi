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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@Tags({"put", "html", "dom", "css", "element"})
@CapabilityDescription("Creates a new HTML element in the input HTML")
@SeeAlso({GetHTMLElement.class, ModifyHTMLElement.class})
public class PutHTMLElement extends AbstractHTMLProcessor {

    public static final String APPEND_ELEMENT = "append-html";
    public static final String PREPEND_ELEMENT = "prepend-html";

    public static final PropertyDescriptor PUT_LOCATION_TYPE = new PropertyDescriptor.Builder()
            .name("Element Insert Location Type")
            .description("Controls whether the new element is prepended or appended to the children of the " +
                    "Element located by the CSS selector. EX: prepended value '<b>Hi</b>' inside of " +
                    "Element (using CSS Selector 'p') '<p>There</p>' would result in " +
                    "'<p><b>Hi</b>There</p>'. Appending the value would result in '<p>There<b>Hi</b></p>'")
            .required(true)
            .allowableValues(APPEND_ELEMENT, PREPEND_ELEMENT)
            .defaultValue(APPEND_ELEMENT)
            .build();

    public static final PropertyDescriptor PUT_VALUE = new PropertyDescriptor.Builder()
            .name("Put Value")
            .description("Value used when creating the new Element. Value should be a valid HTML element. " +
                    "The text should be supplied unencoded: characters like '<', '>', etc will be properly HTML " +
                    "encoded in the output.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(URL);
        descriptors.add(CSS_SELECTOR);
        descriptors.add(HTML_CHARSET);
        descriptors.add(PUT_LOCATION_TYPE);
        descriptors.add(PUT_VALUE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_INVALID_HTML);
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
        if (flowFile == null) {
            return;
        }

        try {
            final Document doc = parseHTMLDocumentFromFlowfile(flowFile, context, session);
            final Elements eles = doc.select(context.getProperty(CSS_SELECTOR).evaluateAttributeExpressions().getValue());

            if (eles == null || eles.size() == 0) {
                //No element found
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                for (Element ele : eles) {
                    switch (context.getProperty(PUT_LOCATION_TYPE).getValue()) {
                        case APPEND_ELEMENT:
                            ele.append(context.getProperty(PUT_VALUE).evaluateAttributeExpressions().getValue());
                            break;
                        case PREPEND_ELEMENT:
                            ele.prepend(context.getProperty(PUT_VALUE).evaluateAttributeExpressions().getValue());
                            break;
                    }
                }

                FlowFile ff = session.write(session.create(flowFile), new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        out.write(doc.html().getBytes());
                    }
                });
                session.transfer(ff, REL_SUCCESS);

                //Transfer the original HTML
                session.transfer(flowFile, REL_ORIGINAL);
            }

        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }

    }

}
