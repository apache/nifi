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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Relationship;

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
    public void write(Class<? extends ConfigurableComponent> configurableComponentClass,
            final OutputStream streamToWriteTo,
            final boolean includesAdditionalDocumentation) throws IOException {

        try {
            XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(
                    streamToWriteTo, "UTF-8");
            xmlStreamWriter.writeDTD("<!DOCTYPE html>");
            xmlStreamWriter.writeStartElement("html");
            xmlStreamWriter.writeAttribute("lang", "en");
            writeHead(configurableComponentClass, xmlStreamWriter);
            writeBody(configurableComponentClass, xmlStreamWriter, includesAdditionalDocumentation);
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.close();
        } catch (XMLStreamException | FactoryConfigurationError e) {
            throw new IOException("Unable to create XMLOutputStream", e);
        }
    }

    /**
     * Writes the head portion of the HTML documentation.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the stream to write to
     * @throws XMLStreamException thrown if there was a problem writing to the
     * stream
     */
    protected void writeHead(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("head");
        xmlStreamWriter.writeStartElement("meta");
        xmlStreamWriter.writeAttribute("charset", "utf-8");
        xmlStreamWriter.writeEndElement();
        writeSimpleElement(xmlStreamWriter, "title", getTitle(configurableComponentClass));

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
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @return the class name of the component
     */
    protected String getTitle(Class<? extends ConfigurableComponent> configurableComponentClass) {
        return configurableComponentClass.getSimpleName();
    }

    /**
     * Writes the body section of the documentation, this consists of the
     * component description, the tags, and the PropertyDescriptors.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the stream writer
     * @param hasAdditionalDetails whether there are additional details present
     * or not
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML stream
     */
    private void writeBody(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails)
            throws XMLStreamException {
        xmlStreamWriter.writeStartElement("body");
        writeDescription(configurableComponentClass, xmlStreamWriter, hasAdditionalDetails);
        writeTags(configurableComponentClass, xmlStreamWriter);
        writeProperties(configurableComponentClass, xmlStreamWriter);
        // writeDynamicProperties(configurableComponentClass, xmlStreamWriter);
        writeAdditionalBodyInfo(configurableComponentClass, xmlStreamWriter);
        writeSeeAlso(configurableComponentClass, xmlStreamWriter);
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Writes the list of components that may be linked from this component.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeSeeAlso(Class<? extends ConfigurableComponent> configurableComponentClass,
            XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final SeeAlso seeAlso = configurableComponentClass.getAnnotation(SeeAlso.class);
        if (seeAlso != null) {
            writeSimpleElement(xmlStreamWriter, "h3", "See Also:");
            xmlStreamWriter.writeStartElement("p");
            int index = 0;
            for (final Class<? extends ConfigurableComponent> linkedComponent : seeAlso.value()) {
                if (index != 0) {
                    xmlStreamWriter.writeCharacters(", ");
                }

                writeLinkForComponent(xmlStreamWriter, linkedComponent);

                ++index;
            }

            for (final String linkedComponent : seeAlso.classNames()) {
                if (index != 0) {
                    xmlStreamWriter.writeCharacters(", ");
                }

                final String link = "../" + linkedComponent + "/index.html";

                final int indexOfLastPeriod = linkedComponent.lastIndexOf(".") + 1;

                writeLink(xmlStreamWriter, linkedComponent.substring(indexOfLastPeriod), link);

                ++index;
            }
            xmlStreamWriter.writeEndElement();
        }
    }

    /**
     * This method may be overridden by sub classes to write additional
     * information to the body of the documentation.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML stream
     */
    protected void writeAdditionalBodyInfo(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

    }

    private void writeTags(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final Tags tags = configurableComponentClass.getAnnotation(Tags.class);
        xmlStreamWriter.writeStartElement("h3");
        xmlStreamWriter.writeCharacters("Tags: ");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeStartElement("p");
        if (tags != null) {
            final String tagString = join(tags.value(), ", ");
            xmlStreamWriter.writeCharacters(tagString);
        } else {
            xmlStreamWriter.writeCharacters("None.");
        }
        xmlStreamWriter.writeEndElement();
    }

    static String join(final String[] toJoin, final String delimiter) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < toJoin.length; i++) {
            sb.append(toJoin[i]);
            if (i < toJoin.length - 1) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    /**
     * Writes a description of the configurable component.
     *
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the stream writer
     * @param hasAdditionalDetails whether there are additional details
     * available as 'additionalDetails.html'
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML stream
     */
    protected void writeDescription(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails)
            throws XMLStreamException {
        writeSimpleElement(xmlStreamWriter, "h2", "Description: ");
        writeSimpleElement(xmlStreamWriter, "p", getDescription(configurableComponentClass));
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
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @return a description of the configurableComponent
     */
    protected String getDescription(Class<? extends ConfigurableComponent> configurableComponentClass) {
        final CapabilityDescription capabilityDescription = configurableComponentClass.getAnnotation(
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
     * @param configurableComponentClass class of {@link ConfigurableComponent}
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML Stream
     */
    protected void writeProperties(Class<? extends ConfigurableComponent> configurableComponentClass,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

        final Collection<PropertyDescriptor> properties = this.retrieveStaticFieldsOfType(configurableComponentClass,
                PropertyDescriptor.class);

        writeSimpleElement(xmlStreamWriter, "h3", "Properties: ");

        if (properties.size() > 0) {
            final boolean containsExpressionLanguage = containsExpressionLanguage(properties);
            final boolean containsSensitiveProperties = containsSensitiveProperties(properties);
            xmlStreamWriter.writeStartElement("p");
            xmlStreamWriter.writeCharacters("In the list below, the names of required properties appear in ");
            writeSimpleElement(xmlStreamWriter, "strong", "bold");
            xmlStreamWriter.writeCharacters(". Any other properties (not in bold) are considered optional. " +
                    "The table also indicates any default values");
            if (containsExpressionLanguage) {
                if (!containsSensitiveProperties) {
                    xmlStreamWriter.writeCharacters(", and ");
                } else {
                    xmlStreamWriter.writeCharacters(", ");
                }
                xmlStreamWriter.writeCharacters("whether a property supports the ");
                writeLink(xmlStreamWriter, "NiFi Expression Language", "../../html/expression-language-guide.html");
            }
            if (containsSensitiveProperties) {
                xmlStreamWriter.writeCharacters(", and whether a property is considered " + "\"sensitive\", meaning that its value will be encrypted. Before entering a "
                        + "value in a sensitive property, ensure that the ");

                writeSimpleElement(xmlStreamWriter, "strong", "nifi.properties");
                xmlStreamWriter.writeCharacters(" file has " + "an entry for the property ");
                writeSimpleElement(xmlStreamWriter, "strong", "nifi.sensitive.props.key");
            }
            xmlStreamWriter.writeCharacters(".");
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "properties");

            // write the header row
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Name");
            writeSimpleElement(xmlStreamWriter, "th", "Default Value");
            writeSimpleElement(xmlStreamWriter, "th", "Allowable Values");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();

            // write the individual properties
            for (PropertyDescriptor property : properties) {
                xmlStreamWriter.writeStartElement("tr");
                xmlStreamWriter.writeStartElement("td");
                xmlStreamWriter.writeAttribute("id", "name");
                if (property.isRequired()) {
                    writeSimpleElement(xmlStreamWriter, "strong", property.getDisplayName());
                } else {
                    xmlStreamWriter.writeCharacters(property.getDisplayName());
                }

                xmlStreamWriter.writeEndElement();
                writeSimpleElement(xmlStreamWriter, "td", property.getDefaultValue(), false, "default-value");
                xmlStreamWriter.writeStartElement("td");
                xmlStreamWriter.writeAttribute("id", "allowable-values");
                writeValidValues(xmlStreamWriter, property);
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.writeStartElement("td");
                xmlStreamWriter.writeAttribute("id", "description");
                if (property.getDescription() != null && property.getDescription().trim().length() > 0) {
                    xmlStreamWriter.writeCharacters(property.getDescription());
                } else {
                    xmlStreamWriter.writeCharacters("No Description Provided.");
                }

                if (property.isSensitive()) {
                    xmlStreamWriter.writeEmptyElement("br");
                    writeSimpleElement(xmlStreamWriter, "strong", "Sensitive Property: true");
                }

                if (property.isExpressionLanguageSupported()) {
                    xmlStreamWriter.writeEmptyElement("br");
                    writeSimpleElement(xmlStreamWriter, "strong", "Supports Expression Language: true");
                }
                xmlStreamWriter.writeEndElement();

                xmlStreamWriter.writeEndElement();
            }

            // TODO support dynamic properties...
            xmlStreamWriter.writeEndElement();

        } else {
            writeSimpleElement(xmlStreamWriter, "p", "This component has no required or optional properties.");
        }
    }

    /**
     * Indicates whether or not the component contains at least one sensitive property.
     *
     * @param propertyDescriptors collection of {@link PropertyDescriptor}s
     * @return whether or not the component contains at least one sensitive property.
     */
    private boolean containsSensitiveProperties(Collection<PropertyDescriptor> propertyDescriptors) {
        for (PropertyDescriptor descriptor : propertyDescriptors) {
            if (descriptor.isSensitive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Indicates whether or not the component contains at least one property that supports Expression Language.
     *
     * @param propertyDescriptors collection of {@link PropertyDescriptor}s
     * @return whether or not the component contains at least one sensitive property.
     */
    private boolean containsExpressionLanguage(Collection<PropertyDescriptor> propertyDescriptors) {
        for (PropertyDescriptor descriptor : propertyDescriptors) {
            if (descriptor.isExpressionLanguageSupported()) {
                return true;
            }
        }
        return false;
    }

    private void writeValidValueDescription(XMLStreamWriter xmlStreamWriter, String description)
            throws XMLStreamException {
        xmlStreamWriter.writeCharacters(" ");
        xmlStreamWriter.writeStartElement("img");
        xmlStreamWriter.writeAttribute("src", "../../html/images/iconInfo.png");
        xmlStreamWriter.writeAttribute("alt", description);
        xmlStreamWriter.writeAttribute("title", description);
        xmlStreamWriter.writeEndElement();

    }

    /**
     * Interrogates a PropertyDescriptor to get a list of AllowableValues, if
     * there are none, nothing is written to the stream.
     *
     * @param xmlStreamWriter the stream writer to use
     * @param property the property to describe
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML Stream
     */
    protected void writeValidValues(XMLStreamWriter xmlStreamWriter, PropertyDescriptor property)
            throws XMLStreamException {
        if (property.getAllowableValues() != null && property.getAllowableValues().size() > 0) {
            xmlStreamWriter.writeStartElement("ul");
            for (AllowableValue value : property.getAllowableValues()) {
                xmlStreamWriter.writeStartElement("li");
                xmlStreamWriter.writeCharacters(value.getDisplayName());

                if (value.getDescription() != null) {
                    writeValidValueDescription(xmlStreamWriter, value.getDescription());
                }
                xmlStreamWriter.writeEndElement();

            }
            xmlStreamWriter.writeEndElement();
        } else if (property.getControllerServiceDefinition() != null) {
            Class<? extends ControllerService> controllerServiceClass = property.getControllerServiceDefinition();

            writeSimpleElement(xmlStreamWriter, "strong", "Controller Service API: ");
            xmlStreamWriter.writeEmptyElement("br");
            xmlStreamWriter.writeCharacters(controllerServiceClass.getSimpleName());

            final List<Class<? extends ControllerService>> implementations = lookupControllerServiceImpls(controllerServiceClass);
            xmlStreamWriter.writeEmptyElement("br");
            if (implementations.size() > 0) {
                final String title = implementations.size() > 1 ? "Implementations: " : "Implementation:";
                writeSimpleElement(xmlStreamWriter, "strong", title);
                for (int i = 0; i < implementations.size(); i++) {
                    xmlStreamWriter.writeEmptyElement("br");
                    writeLinkForComponent(xmlStreamWriter, implementations.get(i));
                }
            } else {
                xmlStreamWriter.writeCharacters("No implementations found.");
            }
        }
    }

    /**
     * Writes a begin element, then text, then end element for the element of a
     * users choosing. Example: &lt;p&gt;text&lt;/p&gt;
     *
     * @param writer the stream writer to use
     * @param elementName the name of the element
     * @param characters the characters to insert into the element
     * @param strong whether the characters should be strong or not.
     * @throws XMLStreamException thrown if there was a problem writing to the
     * stream.
     */
    protected final static void writeSimpleElement(final XMLStreamWriter writer, final String elementName,
            final String characters, boolean strong) throws XMLStreamException {
        writeSimpleElement(writer, elementName, characters, strong, null);
    }

    /**
     * Writes a begin element, an id attribute(if specified), then text, then
     * end element for element of the users choosing. Example: &lt;p
     * id="p-id"&gt;text&lt;/p&gt;
     *
     * @param writer the stream writer to use
     * @param elementName the name of the element
     * @param characters the text of the element
     * @param strong whether to bold the text of the element or not
     * @param id the id of the element. specifying null will cause no element to
     * be written.
     * @throws XMLStreamException xse
     */
    protected final static void writeSimpleElement(final XMLStreamWriter writer, final String elementName,
            final String characters, boolean strong, String id) throws XMLStreamException {
        writer.writeStartElement(elementName);
        if (id != null) {
            writer.writeAttribute("id", id);
        }
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
     * @param writer the stream writer to use
     * @param elementName the name of the element
     * @param characters the characters to insert into the element
     * @throws XMLStreamException thrown if there was a problem writing to the
     * stream
     */
    protected final static void writeSimpleElement(final XMLStreamWriter writer, final String elementName,
            final String characters) throws XMLStreamException {
        writeSimpleElement(writer, elementName, characters, false);
    }

    /**
     * A helper method to write a link
     *
     * @param xmlStreamWriter the stream to write to
     * @param text the text of the link
     * @param location the location of the link
     * @throws XMLStreamException thrown if there was a problem writing to the
     * stream
     */
    protected void writeLink(final XMLStreamWriter xmlStreamWriter, final String text, final String location)
            throws XMLStreamException {
        xmlStreamWriter.writeStartElement("a");
        xmlStreamWriter.writeAttribute("href", location);
        xmlStreamWriter.writeCharacters(text);
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Writes a link to another configurable component
     *
     * @param xmlStreamWriter the xml stream writer
     * @param clazz the configurable component to link to
     * @throws XMLStreamException thrown if there is a problem writing the XML
     */
    protected void writeLinkForComponent(final XMLStreamWriter xmlStreamWriter, final Class<?> clazz) throws XMLStreamException {
        writeLink(xmlStreamWriter, clazz.getSimpleName(), "../" + clazz.getCanonicalName() + "/index.html");
    }

    /**
     * Uses the {@link ExtensionManager} to discover any {@link ControllerService} implementations that implement a specific
     * ControllerService API.
     *
     * @param parent the controller service API
     * @return a list of controller services that implement the controller service API
     */
    private List<Class<? extends ControllerService>> lookupControllerServiceImpls(
            final Class<? extends ControllerService> parent) {

        final List<Class<? extends ControllerService>> implementations = new ArrayList<>();

        // first get all ControllerService implementations
        final Set<Class> controllerServices = ExtensionManager.getExtensions(ControllerService.class);

        // then iterate over all controller services looking for any that is a child of the parent
        // ControllerService API that was passed in as a parameter
        for (final Class<? extends ControllerService> controllerServiceClass : controllerServices) {
            if (parent.isAssignableFrom(controllerServiceClass)) {
                implementations.add(controllerServiceClass);
            }
        }

        return implementations;
    }

    /**
     * Will reflectively retrieve all STATIC fields of type 'fieldType' (e.g.,
     * {@link PropertyDescriptor}, {@link Relationship} etc.) form
     * 'configurableComponentClass' and its super-classes of type
     * {@link ConfigurableComponent}.
     */
    protected <T> Collection<T> retrieveStaticFieldsOfType(Class<? extends ConfigurableComponent> configurableComponentClass, Class<T> fieldType) {
        Set<T> collectedFields = new HashSet<>();
        Class<?> clazz = configurableComponentClass;
        while (clazz != null && ConfigurableComponent.class.isAssignableFrom(clazz)) {
            this.fillStaticFieldsOfType(configurableComponentClass, fieldType, collectedFields);
            clazz = clazz.getSuperclass();
        }
        return collectedFields;
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private <T> void fillStaticFieldsOfType(Class<? extends ConfigurableComponent> configurableComponentClass, Class<T> fieldType, Set<T> collectedFields) {
        Field[] fields = configurableComponentClass.getDeclaredFields();
        try {
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()) && fieldType.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    collectedFields.add((T) field.get(null));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to gather static fields of type '" + fieldType + "' from "
                    + configurableComponentClass, e);
        }
    }
}
