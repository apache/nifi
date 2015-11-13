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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractHTMLProcessor extends AbstractProcessor {

    protected static final String ELEMENT_HTML = "HTML";
    protected static final String ELEMENT_TEXT = "Text";
    protected static final String ELEMENT_DATA = "Data";
    protected static final String ELEMENT_ATTRIBUTE = "Attribute";

    public static final PropertyDescriptor URL = new PropertyDescriptor
            .Builder().name("URL")
            .description("Base URL for the HTML page being parsed.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CSS_SELECTOR = new PropertyDescriptor
            .Builder().name("CSS Selector")
            .description("CSS selector syntax string used to extract the desired HTML element(s).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor HTML_CHARSET = new PropertyDescriptor
            .Builder().name("HTML character encoding")
            .description("Character encoding of the input HTML")
            .defaultValue("UTF-8")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original HTML input")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully parsed HTML element")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to parse HTML content")
            .build();

    public static final Relationship REL_INVALID_HTML = new Relationship.Builder()
            .name("invalid html")
            .description("The input HTML syntax is invalid")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("element not found")
            .description("Element could not be found in the HTML document. The original HTML input will remain " +
                    "in the flowfile content unchanged. Relationship '" + REL_ORIGINAL + "' will not be invoked " +
                    "in this scenario.")
            .build();

    /**
     * Parses the Jsoup HTML document from the FlowFile input content.
     *
     * @param inputFlowFile
     *  Input FlowFile containing the HTML
     *
     * @param context
     *  ProcessContext
     *
     * @param session
     *  ProcessSession
     *
     * @return
     *  Jsoup Document
     */
    protected Document parseHTMLDocumentFromFlowfile(FlowFile inputFlowFile,
                                                                      final ProcessContext context,
                                                                      final ProcessSession session) {
        final AtomicReference<Document> doc = new AtomicReference<>();
        session.read(inputFlowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream inputStream) throws IOException {
                doc.set(Jsoup.parse(inputStream,
                        context.getProperty(HTML_CHARSET).getValue(),
                        context.getProperty(URL).getValue()));
            }
        });
        return doc.get();
    }
}
