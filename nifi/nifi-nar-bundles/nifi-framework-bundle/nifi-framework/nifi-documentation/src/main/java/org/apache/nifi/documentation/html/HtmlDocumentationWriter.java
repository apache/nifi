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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.documentation.DocumentationWriter;

/**
 * Generates HTML documentation for a ConfigurableComponent. This class is used
 * to generate documentation for ControllerService and ReportingTask because
 * they have no additional information.
 * 
 *
 */
public class HtmlDocumentationWriter implements DocumentationWriter {

	/**
	 * The filename where additional user specified information may be stored.
	 */
	public static final String ADDITIONAL_DETAILS_HTML = "additionalDetails.html";

	@Override
	public void write(final ConfigurableComponent configurableComponent, final OutputStream streamToWriteTo,
			final boolean includesAdditionalDocumentation) throws IOException {

		try {
			XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(streamToWriteTo,
					"UTF-8");
			xmlStreamWriter.writeDTD("<!DOCTYPE html>");
			xmlStreamWriter.writeStartElement("html");
			xmlStreamWriter.writeAttribute("lang", "en");
			writeHead(configurableComponent, xmlStreamWriter);
			writeBody(configurableComponent, xmlStreamWriter, includesAdditionalDocumentation);
			xmlStreamWriter.writeEndElement();
			xmlStreamWriter.close();
		} catch (XMLStreamException | FactoryConfigurationError e) {
			throw new IOException("Unable to create XMLOutputStream", e);
		}
	}

	/**
	 * Writes the head portion of the HTML documentation.
	 * 
	 * @param configurableComponent
	 *            the component to describe
	 * @param xmlStreamWriter
	 *            the stream to write to
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the stream
	 */
	protected void writeHead(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter)
			throws XMLStreamException {
		xmlStreamWriter.writeStartElement("head");
		xmlStreamWriter.writeStartElement("meta");
		xmlStreamWriter.writeAttribute("charset", "utf-8");
		xmlStreamWriter.writeEndElement();
		writeSimpleElement(xmlStreamWriter, "title", getTitle(configurableComponent));

		xmlStreamWriter.writeStartElement("link");
		xmlStreamWriter.writeAttribute("rel", "stylesheet");
		xmlStreamWriter.writeAttribute("href", "../../css/component-usage.css");
		xmlStreamWriter.writeAttribute("type", "text/css");
		xmlStreamWriter.writeEndElement();

		xmlStreamWriter.writeEndElement();
	}

	/**
	 * Gets the class name of the component.
	 * 
	 * @param configurableComponent
	 *            the component to describe
	 * @return the class name of the component
	 */
	protected String getTitle(final ConfigurableComponent configurableComponent) {
		return configurableComponent.getClass().getSimpleName();
	}

	/**
	 * Writes the body section of the documentation, this consists of the
	 * component description, the tags, and the PropertyDescriptors.
	 * 
	 * @param configurableComponent
	 *            the component to describe
	 * @param xmlStreamWriter
	 *            the stream writer
	 * @param hasAdditionalDetails
	 *            whether there are additional details present or not
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the XML stream
	 */
	private final void writeBody(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails) throws XMLStreamException {
		xmlStreamWriter.writeStartElement("body");
		writeDescription(configurableComponent, xmlStreamWriter, hasAdditionalDetails);
		writeTags(configurableComponent, xmlStreamWriter);
		writeProperties(configurableComponent, xmlStreamWriter);
		writeAdditionalBodyInfo(configurableComponent, xmlStreamWriter);
		xmlStreamWriter.writeEndElement();
	}

	/**
	 * This method may be overridden by sub classes to write additional
	 * information to the body of the documentation.
	 * 
	 * @param configurableComponent
	 *            the component to describe
	 * @param xmlStreamWriter
	 *            the stream writer
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the XML stream
	 */
	protected void writeAdditionalBodyInfo(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
	}

	/**
	 * Writes the tags attached to a ConfigurableComponent.
	 * 
	 * @param configurableComponent
	 * @param xmlStreamWriter
	 * @throws XMLStreamException
	 */
	private void writeTags(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter)
			throws XMLStreamException {
		final Tags tags = configurableComponent.getClass().getAnnotation(Tags.class);
		xmlStreamWriter.writeStartElement("p");
		if (tags != null) {
			final String tagString = StringUtils.join(tags.value(), ", ");
			xmlStreamWriter.writeCharacters("Tags: ");
			xmlStreamWriter.writeCharacters(tagString);
		} else {
			xmlStreamWriter.writeCharacters("No Tags provided.");
		}
		xmlStreamWriter.writeEndElement();

	}

	/**
	 * Writes a description of the configurable component.
	 * 
	 * @param configurableComponent
	 *            the component to describe
	 * @param xmlStreamWriter
	 *            the stream writer
	 * @param hasAdditionalDetails
	 *            whether there are additional details available as
	 *            'additionalDetails.html'
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the XML stream
	 */
	protected void writeDescription(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails) throws XMLStreamException {
		writeSimpleElement(xmlStreamWriter, "h2", "Description: ");
		writeSimpleElement(xmlStreamWriter, "p", getDescription(configurableComponent));
		if (hasAdditionalDetails) {
			xmlStreamWriter.writeStartElement("p");

			writeLink(xmlStreamWriter, "Additional Details...", ADDITIONAL_DETAILS_HTML);

			xmlStreamWriter.writeEndElement();
		}
	}

	/**
	 * Gets a description of the ConfigurableComponent using the
	 * CapabilityDescription annotation.
	 * 
	 * @param configurableComponent
	 *            the component to describe
	 * @return a description of the configurableComponent
	 */
	protected String getDescription(final ConfigurableComponent configurableComponent) {
		final CapabilityDescription capabilityDescription = configurableComponent.getClass().getAnnotation(
				CapabilityDescription.class);

		final String description;
		if (capabilityDescription != null) {
			description = capabilityDescription.value();
		} else {
			description = "No description provided.";
		}

		return description;
	}

	/**
	 * Writes the PropertyDescriptors out as a table.
	 * 
	 * @param configurableComponent
	 *            the component to describe
	 * @param xmlStreamWriter
	 *            the stream writer
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the XML Stream
	 */
	protected void writeProperties(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
		xmlStreamWriter.writeStartElement("p");
		writeSimpleElement(xmlStreamWriter, "strong", "Properties: ");
		xmlStreamWriter.writeEndElement();
		xmlStreamWriter.writeStartElement("p");
		xmlStreamWriter.writeCharacters("In the list below, the names of required properties appear in ");
		writeSimpleElement(xmlStreamWriter, "strong", "bold");
		xmlStreamWriter.writeCharacters(". Any"
				+ "other properties (not in bold) are considered optional. The table also "
				+ "indicates any default values, whether a property supports the ");
		writeLink(xmlStreamWriter, "NiFi Expression Language (or simply EL)",
				"../../html/expression-language-guide.html");
		xmlStreamWriter.writeCharacters(", and whether a property is considered "
				+ "\"sensitive\", meaning that its value will be encrypted. Before entering a "
				+ "value in a sensitive property, ensure that the ");
		writeSimpleElement(xmlStreamWriter, "strong", "nifi.properties");
		xmlStreamWriter.writeCharacters(" file has " + "an entry for the property ");
		writeSimpleElement(xmlStreamWriter, "strong", "nifi.sensitive.props.key");
		xmlStreamWriter.writeCharacters(".");
		xmlStreamWriter.writeEndElement();

		boolean containsSensitiveElement = false;

		List<PropertyDescriptor> properties = configurableComponent.getPropertyDescriptors();
		if (properties.size() > 0) {
			xmlStreamWriter.writeStartElement("table");

			// write the header row
			xmlStreamWriter.writeStartElement("tr");
			writeSimpleElement(xmlStreamWriter, "th", "Name");
			writeSimpleElement(xmlStreamWriter, "th", "Description");
			writeSimpleElement(xmlStreamWriter, "th", "Default");
			writeSimpleElement(xmlStreamWriter, "th", "Values");
			xmlStreamWriter.writeStartElement("th");
			writeLink(xmlStreamWriter, "EL", "../../html/expression-language-guide.html");
			xmlStreamWriter.writeEndElement();
			xmlStreamWriter.writeEndElement();

			// write the individual properties
			for (PropertyDescriptor property : properties) {
				containsSensitiveElement |= property.isSensitive();
				xmlStreamWriter.writeStartElement("tr");
				xmlStreamWriter.writeStartElement("td");
				if (property.isRequired()) {
					writeSimpleElement(xmlStreamWriter, "strong", property.getDisplayName());
				} else {
					xmlStreamWriter.writeCharacters(property.getDisplayName());
				}
				if (property.isSensitive()) {
					writeSensitiveImg(xmlStreamWriter);
				}
				xmlStreamWriter.writeEndElement();
				writeSimpleElement(xmlStreamWriter, "td", property.getDescription());
				writeSimpleElement(xmlStreamWriter, "td", property.getDefaultValue());
				writeValidValues(xmlStreamWriter, property);
				writeSimpleElement(xmlStreamWriter, "td", property.isExpressionLanguageSupported() ? "Yes" : "No");
				xmlStreamWriter.writeEndElement();
			}

			// TODO support dynamic properties...
			xmlStreamWriter.writeEndElement();
			
			if (containsSensitiveElement) {
				writeSensitiveImg(xmlStreamWriter);
				xmlStreamWriter.writeCharacters(" indicates that a property is a sensitive property");
			}

		} else {
			writeSimpleElement(xmlStreamWriter, "p", "This component has no required or optional properties.");
		}
	}

	private void writeSensitiveImg(final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
		xmlStreamWriter.writeCharacters(" ");
		xmlStreamWriter.writeStartElement("img");
		xmlStreamWriter.writeAttribute("src", "../../html/images/iconSecure.png");
		xmlStreamWriter.writeAttribute("alt", "Sensitive Property");
		xmlStreamWriter.writeEndElement();
	}

	/**
	 * Interrogates a PropertyDescriptor to get a list of AllowableValues, if
	 * there are none, nothing is written to the stream.
	 * 
	 * @param xmlStreamWriter
	 *            the stream writer to use
	 * @param property
	 *            the property to describe
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the XML Stream
	 */
	protected void writeValidValues(XMLStreamWriter xmlStreamWriter, PropertyDescriptor property)
			throws XMLStreamException {
		xmlStreamWriter.writeStartElement("td");
		if (property.getAllowableValues() != null && property.getAllowableValues().size() > 0) {
			xmlStreamWriter.writeStartElement("ul");
			for (AllowableValue value : property.getAllowableValues()) {
				writeSimpleElement(xmlStreamWriter, "li", value.getDisplayName());
			}
			xmlStreamWriter.writeEndElement();
		}
		xmlStreamWriter.writeEndElement();
	}

	/**
	 * Writes a begin element, then text, then end element for the element of a
	 * users choosing. Example: &lt;p&gt;text&lt;/p&gt;
	 * 
	 * @param writer
	 *            the stream writer to use
	 * @param elementName
	 *            the name of the element
	 * @param characters
	 *            the characters to insert into the element
	 * @param strong
	 *            whether the characters should be strong or not.
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the stream.
	 */
	protected final static void writeSimpleElement(final XMLStreamWriter writer, final String elementName,
			final String characters, boolean strong) throws XMLStreamException {
		writer.writeStartElement(elementName);
		if (strong) {
			writer.writeStartElement("strong");
		}
		writer.writeCharacters(characters);
		if (strong) {
			writer.writeEndElement();
		}
		writer.writeEndElement();
	}

	/**
	 * Writes a begin element, then text, then end element for the element of a
	 * users choosing. Example: &lt;p&gt;text&lt;/p&gt;
	 * 
	 * @param writer
	 *            the stream writer to use
	 * @param elementName
	 *            the name of the element
	 * @param characters
	 *            the characters to insert into the element
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the stream
	 */
	protected final static void writeSimpleElement(final XMLStreamWriter writer, final String elementName,
			final String characters) throws XMLStreamException {
		writeSimpleElement(writer, elementName, characters, false);
	}

	/**
	 * A helper method to write a link
	 * 
	 * @param xmlStreamWriter
	 *            the stream to write to
	 * @param text
	 *            the text of the link
	 * @param location
	 *            the location of the link
	 * @throws XMLStreamException
	 *             thrown if there was a problem writing to the stream
	 */
	protected void writeLink(final XMLStreamWriter xmlStreamWriter, final String text, final String location)
			throws XMLStreamException {
		xmlStreamWriter.writeStartElement("a");
		xmlStreamWriter.writeAttribute("href", location);
		xmlStreamWriter.writeCharacters(text);
		xmlStreamWriter.writeEndElement();
	}
}
