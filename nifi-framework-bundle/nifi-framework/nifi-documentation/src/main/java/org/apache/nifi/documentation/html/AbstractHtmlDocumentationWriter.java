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

import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.util.StringUtils;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class AbstractHtmlDocumentationWriter<T> implements DocumentationWriter<T> {

    /**
     * The filename where additional user specified information may be stored.
     */
    public static final String ADDITIONAL_DETAILS_HTML = "additionalDetails.html";

    static final String NO_DESCRIPTION = "No description provided.";
    static final String NO_TAGS = "No tags provided.";
    static final String NO_PROPERTIES = "This component has no required or optional properties.";

    static final String H2 = "h2";
    static final String H3 = "h3";
    static final String H4 = "h4";
    static final String P = "p";
    static final String BR = "br";
    static final String SPAN = "span";
    static final String STRONG = "strong";
    static final String TABLE = "table";
    static final String TH = "th";
    static final String TR = "tr";
    static final String TD = "td";
    static final String UL = "ul";
    static final String LI = "li";
    static final String ID = "id";

    @Override
    public void write(final T component, final OutputStream outputStream, final boolean includesAdditionalDocumentation) throws IOException {
        try {
            XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, "UTF-8");
            xmlStreamWriter.writeDTD("<!DOCTYPE html>");
            xmlStreamWriter.writeStartElement("html");
            xmlStreamWriter.writeAttribute("lang", "en");
            writeHead(component, xmlStreamWriter);
            writeBody(component, xmlStreamWriter, includesAdditionalDocumentation);
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.close();
        } catch (XMLStreamException | FactoryConfigurationError e) {
            throw new IOException("Unable to create XMLOutputStream", e);
        }
    }

    /**
     * Writes the head portion of the HTML documentation.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream to write to
     * @throws XMLStreamException thrown if there was a problem writing to the stream
     */
    protected void writeHead(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("head");
        xmlStreamWriter.writeStartElement("meta");
        xmlStreamWriter.writeAttribute("charset", "utf-8");
        xmlStreamWriter.writeEndElement();
        writeSimpleElement(xmlStreamWriter, "title", getTitle(component));

        xmlStreamWriter.writeStartElement("link");
        xmlStreamWriter.writeAttribute("rel", "stylesheet");
        xmlStreamWriter.writeAttribute("href", "../../../../../css/component-usage.css");
        xmlStreamWriter.writeAttribute("type", "text/css");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("script");
        xmlStreamWriter.writeAttribute("type", "text/javascript");
        xmlStreamWriter.writeCharacters("window.onload = function(){if(self==top) { " +
                "document.getElementById('nameHeader').style.display = \"inherit\"; } }");
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Writes the body section of the documentation, this consists of the component description, the tags, and the PropertyDescriptors.
     *
     * @param component            the component to describe
     * @param xmlStreamWriter      the stream writer
     * @param hasAdditionalDetails whether there are additional details present or not
     * @throws XMLStreamException thrown if there was a problem writing to the XML stream
     */
    void writeBody(final T component, final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("body");
        writeHeader(component, xmlStreamWriter);
        writeDeprecationWarning(component, xmlStreamWriter);
        writeDescription(component, xmlStreamWriter, hasAdditionalDetails);
        writeTags(component, xmlStreamWriter);
        writeProperties(component, xmlStreamWriter);
        writeDynamicProperties(component, xmlStreamWriter);
        writeAdditionalBodyInfo(component, xmlStreamWriter);
        writeStatefulInfo(component, xmlStreamWriter);
        writeRestrictedInfo(component, xmlStreamWriter);
        writeInputRequirementInfo(component, xmlStreamWriter);
        writeUseCases(component, xmlStreamWriter);
        writeMultiComponentUseCases(component, xmlStreamWriter);
        writeSystemResourceConsiderationInfo(component, xmlStreamWriter);
        writeSeeAlso(component, xmlStreamWriter);
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Write the header to be displayed when loaded outside an iframe.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeHeader(final T component, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("h1");
        xmlStreamWriter.writeAttribute(ID, "nameHeader");
        // Style will be overwritten on load if needed
        xmlStreamWriter.writeAttribute("style", "display: none;");
        xmlStreamWriter.writeCharacters(getTitle(component));
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Writes a description of the component.
     *
     * @param component            the component to describe
     * @param xmlStreamWriter      the stream writer
     * @param hasAdditionalDetails whether there are additional details available as 'additionalDetails.html'
     * @throws XMLStreamException thrown if there was a problem writing to the XML stream
     */
    protected void writeDescription(final T component, final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails) throws XMLStreamException {
        writeSimpleElement(xmlStreamWriter, H2, "Description: ");
        writeSimpleElement(xmlStreamWriter, P, getDescription(component));
        if (hasAdditionalDetails) {
            xmlStreamWriter.writeStartElement(P);

            writeLink(xmlStreamWriter, "Additional Details...", ADDITIONAL_DETAILS_HTML);

            xmlStreamWriter.writeEndElement();
        }
    }

    /**
     * This method may be overridden by subclasses to write additional information to the body of the documentation.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the XML stream
     */
    protected void writeAdditionalBodyInfo(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

    }

    /**
     * Writes a begin element, an id attribute(if specified), then text, then end element for element of the users choosing. Example: &lt;p
     * id="p-id"&gt;text&lt;/p&gt;
     *
     * @param writer      the stream writer to use
     * @param elementName the name of the element
     * @param characters  the text of the element
     * @param id          the id of the element. specifying null will cause no element to be written.
     * @throws XMLStreamException xse
     */
    protected static void writeSimpleElement(final XMLStreamWriter writer, final String elementName, final String characters, String id) throws XMLStreamException {
        writer.writeStartElement(elementName);

        if (characters != null) {
            if (id != null) {
                writer.writeAttribute(ID, id);
            }
            writer.writeCharacters(characters);
        }

        writer.writeEndElement();
    }

    /**
     * Writes a begin element, then text, then end element for the element of a users choosing. Example: &lt;p&gt;text&lt;/p&gt;
     *
     * @param writer      the stream writer to use
     * @param elementName the name of the element
     * @param characters  the characters to insert into the element
     */
    protected static void writeSimpleElement(final XMLStreamWriter writer, final String elementName, final String characters) throws XMLStreamException {
        writeSimpleElement(writer, elementName, characters, null);
    }

    /**
     * A helper method to write a link
     *
     * @param xmlStreamWriter the stream to write to
     * @param text            the text of the link
     * @param location        the location of the link
     * @throws XMLStreamException thrown if there was a problem writing to the
     *                            stream
     */
    protected void writeLink(final XMLStreamWriter xmlStreamWriter, final String text, final String location) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("a");
        xmlStreamWriter.writeAttribute("href", location);
        xmlStreamWriter.writeCharacters(text);
        xmlStreamWriter.writeEndElement();
    }

    void writeUseCaseConfiguration(final String configuration, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        if (StringUtils.isEmpty(configuration)) {
            return;
        }

        writeSimpleElement(xmlStreamWriter, H4, "Configuration:");

        final String[] splits = configuration.split("\\n");
        for (final String split : splits) {
            xmlStreamWriter.writeStartElement(P);

            final Matcher matcher = Pattern.compile("`(.*?)`").matcher(split);
            int startIndex = 0;
            while (matcher.find()) {
                final int start = matcher.start();
                if (start > 0) {
                    xmlStreamWriter.writeCharacters(split.substring(startIndex, start));
                }

                writeSimpleElement(xmlStreamWriter, "code", matcher.group(1));

                startIndex = matcher.end();
            }

            if (split.length() > startIndex) {
                if (startIndex == 0) {
                    xmlStreamWriter.writeCharacters(split);
                } else {
                    xmlStreamWriter.writeCharacters(split.substring(startIndex));
                }
            }

            xmlStreamWriter.writeEndElement();
        }
    }

    /**
     * Gets the class name of the component.
     *
     * @param component the component to describe
     * @return the class name of the component
     */
    abstract String getTitle(final T component);

    /**
     * Writes a warning about the deprecation of a component.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the XML stream
     */
    abstract void writeDeprecationWarning(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    /**
     * Gets a description of the component using the CapabilityDescription annotation.
     *
     * @param component the component to describe
     * @return a description of the component
     */
    abstract String getDescription(final T component);

    /**
     * Writes the tag list of the component.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the XML stream
     */
    abstract void writeTags(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    /**
     * Writes the PropertyDescriptors out as a table.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the XML Stream
     */
    abstract void writeProperties(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    abstract void writeDynamicProperties(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    /**
     * Write the description of the Stateful annotation if provided in this component.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    abstract void writeStatefulInfo(final T component, XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    /**
     * Write the description of the Restricted annotation if provided in this component.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    abstract void writeRestrictedInfo(final T component, XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    /**
     * Add in the documentation information regarding the component whether it accepts an incoming relationship or not.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    abstract void writeInputRequirementInfo(final T component, XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    abstract void writeUseCases(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    abstract void writeMultiComponentUseCases(final T component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    /**
     * Writes the list of components that may be linked from this component.
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    abstract void writeSeeAlso(final T component, XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

    /**
     * Writes all the system resource considerations for this component
     *
     * @param component       the component to describe
     * @param xmlStreamWriter the xml stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    abstract void writeSystemResourceConsiderationInfo(final T component, XMLStreamWriter xmlStreamWriter) throws XMLStreamException;

}
