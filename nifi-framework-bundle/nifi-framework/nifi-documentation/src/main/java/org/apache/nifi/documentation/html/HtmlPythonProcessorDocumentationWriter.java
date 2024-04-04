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
package org.apache.nifi.documentation.html;

import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.documentation.MultiProcessorUseCaseDetails;
import org.apache.nifi.python.processor.documentation.ProcessorConfigurationDetails;
import org.apache.nifi.python.processor.documentation.PropertyDescription;
import org.apache.nifi.python.processor.documentation.UseCaseDetails;
import org.apache.nifi.util.StringUtils;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.List;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

public class HtmlPythonProcessorDocumentationWriter extends AbstractHtmlDocumentationWriter<PythonProcessorDetails> {

    @Override
    String getTitle(final PythonProcessorDetails processorDetails) {
        return processorDetails.getProcessorType();
    }

    @Override
    void writeDeprecationWarning(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) {
        // Not supported
    }

    @Override
    String getDescription(final PythonProcessorDetails processorDetails) {
        return processorDetails.getCapabilityDescription() != null ? processorDetails.getCapabilityDescription() : NO_DESCRIPTION;
    }

    @Override
    void writeInputRequirementInfo(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) {
        // Not supported
    }

    @Override
    void writeStatefulInfo(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) {
        // Not supported
    }

    @Override
    void writeRestrictedInfo(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) {
        // Not supported
    }

    @Override
    void writeSeeAlso(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) {
        // Not supported
    }

    @Override
    void writeTags(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<String> tags = processorDetails.getTags();

        xmlStreamWriter.writeStartElement(H3);
        xmlStreamWriter.writeCharacters("Tags: ");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeStartElement(P);

        if (tags != null && !tags.isEmpty()) {
            final String tagString = String.join(", ", tags);
            xmlStreamWriter.writeCharacters(tagString);
        } else {
            xmlStreamWriter.writeCharacters(NO_TAGS);
        }

        xmlStreamWriter.writeEndElement();
    }

    @Override
    void writeUseCases(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<UseCaseDetails> useCaseDetailsList = processorDetails.getUseCases();
        if (useCaseDetailsList.isEmpty()) {
            return;
        }

        writeSimpleElement(xmlStreamWriter, H2, "Example Use Cases:");

        for (final UseCaseDetails useCaseDetails : useCaseDetailsList) {
            writeSimpleElement(xmlStreamWriter, H3, "Use Case:");
            writeSimpleElement(xmlStreamWriter, P,  useCaseDetails.getDescription());

            final String notes = useCaseDetails.getNotes();
            if (!StringUtils.isEmpty(notes)) {
                writeSimpleElement(xmlStreamWriter, H4, "Notes:");

                final String[] splits = notes.split("\\n");
                for (final String split : splits) {
                    writeSimpleElement(xmlStreamWriter, P, split);
                }
            }

            final List<String> keywords = useCaseDetails.getKeywords();
            if (!keywords.isEmpty()) {
                writeSimpleElement(xmlStreamWriter, H4, "Keywords:");
                xmlStreamWriter.writeCharacters(String.join(", ", keywords));
            }

            final String configuration = useCaseDetails.getConfiguration();
            writeUseCaseConfiguration(configuration, xmlStreamWriter);

            writeSimpleElement(xmlStreamWriter, BR, null);
        }
    }

    @Override
    void writeMultiComponentUseCases(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<MultiProcessorUseCaseDetails> useCaseDetailsList = processorDetails.getMultiProcessorUseCases();
        if (useCaseDetailsList.isEmpty()) {
            return;
        }

        writeSimpleElement(xmlStreamWriter, H2, "Example Use Cases Involving Other Components:");

        for (final MultiProcessorUseCaseDetails useCase : useCaseDetailsList) {
            writeSimpleElement(xmlStreamWriter, H3, "Use Case:");
            writeSimpleElement(xmlStreamWriter, P,  useCase.getDescription());

            final String notes = useCase.getNotes();
            if (!StringUtils.isEmpty(notes)) {
                writeSimpleElement(xmlStreamWriter, H4, "Notes:");

                final String[] splits = notes.split("\\n");
                for (final String split : splits) {
                    writeSimpleElement(xmlStreamWriter, P, split);
                }
            }

            final List<String> keywords = useCase.getKeywords();
            if (!keywords.isEmpty()) {
                writeSimpleElement(xmlStreamWriter, H4, "Keywords:");
                xmlStreamWriter.writeCharacters(String.join(", ", keywords));
            }

            writeSimpleElement(xmlStreamWriter, H4, "Components involved:");
            final List<ProcessorConfigurationDetails> processorConfigurations = useCase.getConfigurations();
            for (final ProcessorConfigurationDetails processorConfiguration : processorConfigurations) {
                writeSimpleElement(xmlStreamWriter, STRONG, "Component Type: ");
                writeSimpleElement(xmlStreamWriter, SPAN, processorConfiguration.getProcessorType());

                final String configuration = processorConfiguration.getConfiguration();
                writeUseCaseConfiguration(configuration, xmlStreamWriter);

                writeSimpleElement(xmlStreamWriter, BR, null);
            }

            writeSimpleElement(xmlStreamWriter, BR, null);
        }
    }

    @Override
    void writeProperties(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<PropertyDescription> properties = processorDetails.getPropertyDescriptions();
        writeSimpleElement(xmlStreamWriter, H3, "Properties: ");

        if (!properties.isEmpty()) {
            final boolean containsExpressionLanguage = containsExpressionLanguage(processorDetails);
            xmlStreamWriter.writeStartElement(P);
            xmlStreamWriter.writeCharacters("In the list below, the names of required properties appear in ");
            writeSimpleElement(xmlStreamWriter, STRONG, "bold");
            xmlStreamWriter.writeCharacters(". Any other properties (not in bold) are considered optional. " +
                    "The table also indicates any default values");
            if (containsExpressionLanguage) {
                xmlStreamWriter.writeCharacters(", and whether a property supports the ");
                writeLink(xmlStreamWriter, "NiFi Expression Language", "../../../../../html/expression-language-guide.html");
            }
            xmlStreamWriter.writeCharacters(".");
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeStartElement(TABLE);
            xmlStreamWriter.writeAttribute(ID, "properties");

            // write the header row
            xmlStreamWriter.writeStartElement(TR);
            writeSimpleElement(xmlStreamWriter, TH, "Display Name");
            writeSimpleElement(xmlStreamWriter, TH, "API Name");
            writeSimpleElement(xmlStreamWriter, TH, "Default Value");
            writeSimpleElement(xmlStreamWriter, TH, "Description");
            xmlStreamWriter.writeEndElement();

            // write the individual properties
            for (PropertyDescription property : properties) {
                xmlStreamWriter.writeStartElement(TR);
                xmlStreamWriter.writeStartElement(TD);
                xmlStreamWriter.writeAttribute(ID, "name");
                if (property.isRequired()) {
                    writeSimpleElement(xmlStreamWriter, STRONG, property.getDisplayName());
                } else {
                    xmlStreamWriter.writeCharacters(property.getDisplayName());
                }

                xmlStreamWriter.writeEndElement();
                writeSimpleElement(xmlStreamWriter, TD, property.getName());
                writeSimpleElement(xmlStreamWriter, TD, property.getDefaultValue(), "default-value");
                xmlStreamWriter.writeStartElement(TD);
                xmlStreamWriter.writeAttribute(ID, "description");
                if (property.getDescription() != null && !property.getDescription().trim().isEmpty()) {
                    xmlStreamWriter.writeCharacters(property.getDescription());
                } else {
                    xmlStreamWriter.writeCharacters(NO_DESCRIPTION);
                }

                if (property.isSensitive()) {
                    xmlStreamWriter.writeEmptyElement(BR);
                    writeSimpleElement(xmlStreamWriter, STRONG, "Sensitive Property: true");
                }

                final ExpressionLanguageScope expressionLanguageScope = ExpressionLanguageScope.valueOf(property.getExpressionLanguageScope());
                if (!expressionLanguageScope.equals(NONE)) {
                    xmlStreamWriter.writeEmptyElement(BR);
                    String text = "Supports Expression Language: true";
                    final String perFF = " (will be evaluated using flow file attributes and Environment variables)";
                    final String registry = " (will be evaluated using Environment variables only)";
                    final String undefined = " (undefined scope)";

                    switch (expressionLanguageScope) {
                        case FLOWFILE_ATTRIBUTES -> text += perFF;
                        case ENVIRONMENT -> text += registry;
                        default -> text += undefined;
                    }

                    writeSimpleElement(xmlStreamWriter, STRONG, text);
                }

                xmlStreamWriter.writeEndElement();

                xmlStreamWriter.writeEndElement();
            }

            xmlStreamWriter.writeEndElement();

        } else {
            writeSimpleElement(xmlStreamWriter, P, NO_PROPERTIES);
        }
    }

    @Override
    void writeDynamicProperties(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) {
        // Not supported
    }

    @Override
    void writeSystemResourceConsiderationInfo(final PythonProcessorDetails processorDetails, XMLStreamWriter xmlStreamWriter) {
        // Not supported
    }

    /**
     * Indicates whether the component contains at least one property that supports Expression Language.
     *
     * @param processorDetails the component to interrogate
     * @return whether the component contains at least one sensitive property.
     */
    private boolean containsExpressionLanguage(final PythonProcessorDetails processorDetails) {
        for (PropertyDescription description : processorDetails.getPropertyDescriptions()) {
            if (!ExpressionLanguageScope.valueOf(description.getExpressionLanguageScope()).equals(NONE)) {
                return true;
            }
        }
        return false;
    }
}