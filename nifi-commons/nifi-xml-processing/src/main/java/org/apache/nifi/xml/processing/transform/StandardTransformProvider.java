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
package org.apache.nifi.xml.processing.transform;

import org.apache.nifi.xml.processing.ProcessingAttribute;
import org.apache.nifi.xml.processing.ProcessingException;

import javax.xml.XMLConstants;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import java.util.Objects;

/**
 * Standard implementation of Transform Provider with secure processing enabled
 */
public class StandardTransformProvider implements TransformProvider {
    private static final boolean SECURE_PROCESSING_ENABLED = true;

    private static final String ENABLED_PROPERTY = "yes";

    private static final String INDENT_AMOUNT_OUTPUT_KEY = "{http://xml.apache.org/xslt}indent-amount";

    private static final String INDENT_AMOUNT = "2";

    private boolean indent;

    private boolean omitXmlDeclaration;

    private String method;

    /**
     * Set Indent Status
     *
     * @param indent Indent Status
     */
    public void setIndent(final boolean indent) {
        this.indent = indent;
    }

    /**
     * Set Output Method
     *
     * @param method Method or null when default configuration should be used
     */
    public void setMethod(final String method) {
        this.method = method;
    }

    /**
     * Set Omit XML Declaration
     *
     * @param omitXmlDeclaration Omit XML Declaration
     */
    public void setOmitXmlDeclaration(final boolean omitXmlDeclaration) {
        this.omitXmlDeclaration = omitXmlDeclaration;
    }

    /**
     * Transform Source to Result
     *
     * @param source Source to be transformed
     * @param result Result containing transformed information
     */
    @Override
    public void transform(final Source source, final Result result) {
        Objects.requireNonNull(source, "Source required");
        Objects.requireNonNull(result, "Result required");

        final TransformerFactory transformerFactory = TransformerFactory.newInstance();
        final Transformer transformer;
        try {
            transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ProcessingAttribute.ACCESS_EXTERNAL_DTD.getValue());
            transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, ProcessingAttribute.ACCESS_EXTERNAL_STYLESHEET.getValue());
            transformerFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, SECURE_PROCESSING_ENABLED);
            transformer = transformerFactory.newTransformer();
        } catch (final TransformerConfigurationException e) {
            throw new ProcessingException("Transformer configuration failed", e);
        }

        if (indent) {
            transformer.setOutputProperty(OutputKeys.INDENT, ENABLED_PROPERTY);
            transformer.setOutputProperty(INDENT_AMOUNT_OUTPUT_KEY, INDENT_AMOUNT);
        }

        if (method != null) {
            transformer.setOutputProperty(OutputKeys.METHOD, method);
        }

        if (omitXmlDeclaration) {
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, ENABLED_PROPERTY);
        }

        try {
            transformer.transform(source, result);
        } catch (final TransformerException e) {
            throw new ProcessingException("Transform failed", e);
        }
    }
}
