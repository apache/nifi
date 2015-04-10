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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"xml", "xslt", "transform"})
@CapabilityDescription("Applies the provided XSLT file to the flowfile XML payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the XSL transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship")
@DynamicProperty(name="An XSLT transform parameter name", value="An XSLT transform parameter value", supportsExpressionLanguage=true, 
description="These XSLT parameters are passed to the transformer")
public class TransformXml extends AbstractProcessor {

    public static final PropertyDescriptor XSLT_FILE_NAME = new PropertyDescriptor.Builder()
            .name("XSLT file name")
            .description("Provides the name (including full path) of the XSLT file to apply to the flowfile XML content.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("The FlowFile with transformed content will be routed to this relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(XSLT_FILE_NAME);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        try {
            FlowFile transformed = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream out) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {

                        File stylesheet = new File(context.getProperty(XSLT_FILE_NAME).getValue());
                        StreamSource styleSource = new StreamSource(stylesheet);
                        TransformerFactory tfactory = new net.sf.saxon.TransformerFactoryImpl();
                        Transformer transformer = tfactory.newTransformer(styleSource);

                        // pass all dynamic properties to the transformer
                        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                            if (entry.getKey().isDynamic()) {
                                String value = context.newPropertyValue(entry.getValue()).evaluateAttributeExpressions(original).getValue();
                                transformer.setParameter(entry.getKey().getName(), value);
                            }
                        }

                        // use a StreamSource with Saxon
                        StreamSource source = new StreamSource(in);
                        StreamResult result = new StreamResult(out);
                        transformer.transform(source, result);
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                }
            });
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("Transformed {}", new Object[]{original});
        } catch (ProcessException e) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }
    }

    @SuppressWarnings("unused")
    private static final class XsltValidator implements Validator {

        private volatile Tuple<String, ValidationResult> cachedResult;

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            final Tuple<String, ValidationResult> lastResult = this.cachedResult;
            if (lastResult != null && lastResult.getKey().equals(input)) {
                return lastResult.getValue();
            } else {
                String error = null;
                final File stylesheet = new File(input);
                final TransformerFactory tfactory = new net.sf.saxon.TransformerFactoryImpl();
                final StreamSource styleSource = new StreamSource(stylesheet);

                try {
                    tfactory.newTransformer(styleSource);
                } catch (final Exception e) {
                    error = e.toString();
                }

                this.cachedResult = new Tuple<>(input,
                        new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(error == null)
                        .explanation(error).build());
                return this.cachedResult.getValue();
            }
        }
    }

}
