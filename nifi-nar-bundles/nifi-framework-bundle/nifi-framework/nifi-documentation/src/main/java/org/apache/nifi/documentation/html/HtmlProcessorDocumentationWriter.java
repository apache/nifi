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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.processor.Relationship;

/**
 * Writes documentation specific for a Processor. This includes everything for a
 * ConfigurableComponent as well as Relationship information.
 *
 *
 */
public class HtmlProcessorDocumentationWriter extends HtmlDocumentationWriter {

    @Override
    protected void writeAdditionalBodyInfo(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        writeRelationships(configurableComponentClass, xmlStreamWriter);
        writeAttributeInfo(configurableComponentClass, xmlStreamWriter);
    }

    /**
     * Writes all the attributes that a processor says it reads and writes
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the xml stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeAttributeInfo(Class<? extends ConfigurableComponent> configurableComponentClass,
            XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {

        handleReadsAttributes(xmlStreamWriter, configurableComponentClass);
        handleWritesAttributes(xmlStreamWriter, configurableComponentClass);
    }

    private String defaultIfBlank(final String test, final String defaultValue) {
        if (test == null || test.trim().isEmpty()) {
            return defaultValue;
        }
        return test;
    }

    /**
     * Writes out just the attributes that are being read in a table form.
     *
     * @param xmlStreamWriter the xml stream writer to use
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @throws XMLStreamException xse
     */
    private void handleReadsAttributes(XMLStreamWriter xmlStreamWriter,
            Class<? extends ConfigurableComponent> configurableComponentClass)
            throws XMLStreamException {
        List<ReadsAttribute> attributesRead = getReadsAttributes(configurableComponentClass);

        writeSimpleElement(xmlStreamWriter, "h3", "Reads Attributes: ");
        if (attributesRead.size() > 0) {
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "reads-attributes");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Name");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();
            for (ReadsAttribute attribute : attributesRead) {
                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "td",
                        defaultIfBlank(attribute.attribute(), "Not Specified"));
                // TODO allow for HTML characters here.
                writeSimpleElement(xmlStreamWriter, "td",
                        defaultIfBlank(attribute.description(), "Not Specified"));
                xmlStreamWriter.writeEndElement();

            }
            xmlStreamWriter.writeEndElement();

        } else {
            xmlStreamWriter.writeCharacters("None specified.");
        }
    }

    /**
     * Writes out just the attributes that are being written to in a table form.
     *
     * @param xmlStreamWriter the xml stream writer to use
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @throws XMLStreamException xse
     */
    private void handleWritesAttributes(XMLStreamWriter xmlStreamWriter, Class<? extends ConfigurableComponent> configurableComponentClass)
            throws XMLStreamException {
        List<WritesAttribute> attributesRead = getWritesAttributes(configurableComponentClass);

        writeSimpleElement(xmlStreamWriter, "h3", "Writes Attributes: ");
        if (attributesRead.size() > 0) {
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "writes-attributes");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Name");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();
            for (WritesAttribute attribute : attributesRead) {
                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "td",
                        defaultIfBlank(attribute.attribute(), "Not Specified"));
                // TODO allow for HTML characters here.
                writeSimpleElement(xmlStreamWriter, "td",
                        defaultIfBlank(attribute.description(), "Not Specified"));
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndElement();

        } else {
            xmlStreamWriter.writeCharacters("None specified.");
        }
    }

    /**
     * Collects the attributes that a processor is reading from.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @return the list of attributes that processor is reading
     */
    private List<ReadsAttribute> getReadsAttributes(Class<? extends ConfigurableComponent> configurableComponentClass) {
        List<ReadsAttribute> attributes = new ArrayList<>();

        ReadsAttributes readsAttributes = configurableComponentClass.getAnnotation(ReadsAttributes.class);
        if (readsAttributes != null) {
            attributes.addAll(Arrays.asList(readsAttributes.value()));
        }

        ReadsAttribute readsAttribute = configurableComponentClass.getAnnotation(ReadsAttribute.class);
        if (readsAttribute != null) {
            attributes.add(readsAttribute);
        }

        return attributes;
    }

    /**
     * Collects the attributes that a processor is writing to.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @return the list of attributes the processor is writing
     */
    private List<WritesAttribute> getWritesAttributes(Class<? extends ConfigurableComponent> configurableComponentClass) {
        List<WritesAttribute> attributes = new ArrayList<>();

        WritesAttributes writesAttributes = configurableComponentClass.getAnnotation(WritesAttributes.class);
        if (writesAttributes != null) {
            attributes.addAll(Arrays.asList(writesAttributes.value()));
        }

        WritesAttribute writeAttribute = configurableComponentClass.getAnnotation(WritesAttribute.class);
        if (writeAttribute != null) {
            attributes.add(writeAttribute);
        }

        return attributes;
    }

    /**
     * Writes a table describing the relations a processor has.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the xml
     */
    private void writeRelationships(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {

        writeSimpleElement(xmlStreamWriter, "h3", "Relationships: ");

        Collection<Relationship> relationships = this.retrieveStaticFieldsOfType(configurableComponentClass,
                Relationship.class);

        if (relationships.size() > 0) {
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "relationships");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Name");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();

            for (Relationship relationship : relationships) {
                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "td", relationship.getName());
                writeSimpleElement(xmlStreamWriter, "td", relationship.getDescription());
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndElement();
        } else {
            xmlStreamWriter.writeCharacters("This processor has no relationships.");
        }
    }
}
