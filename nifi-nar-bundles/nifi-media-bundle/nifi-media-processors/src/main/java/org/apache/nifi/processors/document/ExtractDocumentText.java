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
package org.apache.nifi.processors.document;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.BufferedInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"extract, text, pdf, word, excel, powerpoint, office"})
@CapabilityDescription("Run Apache Tika text extraction to extra the text from supported binary file formats such as PDF " +
        "and Microsoft Office files.")
public class ExtractDocumentText extends AbstractProcessor {
    private static final String TEXT_PLAIN = "text/plain";

    public static final String FIELD_MAX_TEXT_LENGTH = "MAX_TEXT_LENGTH";
    public static final String FIELD_SUCCESS = "success";
    public static final String FIELD_FAILURE = "failure";

    public static final PropertyDescriptor MAX_TEXT_LENGTH = new PropertyDescriptor.Builder()
            .name(FIELD_MAX_TEXT_LENGTH)
            .displayName("Max Output Text Length")
            .description("The maximum length of text to retrieve. This is used to limit memory usage for " +
                    "dealing with large files. Specify -1 for unlimited length.")
            .required(false).defaultValue("-1").addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name(FIELD_SUCCESS)
            .description("Successfully extract content.").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name(FIELD_FAILURE)
            .description("Failed to extract content.").build();

    private List<PropertyDescriptor> descriptors = Collections.unmodifiableList(Arrays.asList(MAX_TEXT_LENGTH));
    private Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        return;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        final int maxTextLength = context.getProperty(MAX_TEXT_LENGTH).evaluateAttributeExpressions(flowFile).asInteger();
        final String filename = flowFile.getAttribute("filename");

        try {
            final AtomicReference<String> type = new AtomicReference<>();
            final AtomicReference<Boolean> wasError = new AtomicReference<>(false);

            flowFile = session.write(flowFile, (inputStream, outputStream) -> {
                if (inputStream != null) {
                    BufferedInputStream buffStream = new BufferedInputStream(inputStream);
                    Tika tika = new Tika();
                    String text = "";
                    try {
                        type.set(tika.detect(buffStream, filename));
                        tika.setMaxStringLength(maxTextLength);
                        text = tika.parseToString(buffStream);

                    } catch (TikaException e) {
                        getLogger().error("Apache Tika failed to parse input " + e.getLocalizedMessage());
                        wasError.set(true);
                    }

                    outputStream.write(text.getBytes());
                    buffStream.close();
                } else {
                    getLogger().error("Input file was null");
                    wasError.set(true);
                }
            });

            if (wasError.get()) {
                session.transfer(flowFile, REL_FAILURE);
            } else {

                Map<String, String> mimeAttrs = new HashMap<>();
                mimeAttrs.put("mime.type", TEXT_PLAIN);
                mimeAttrs.put("orig.mime.type", type.get());

                flowFile = session.putAllAttributes(flowFile, mimeAttrs);
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (final Throwable t) {
            getLogger().error("Unable to process ExtractTextProcessor file " + t.getLocalizedMessage());
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            // not sure about this one
            session.transfer(flowFile, REL_FAILURE);
            throw t;
        }
    }
}
