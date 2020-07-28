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

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Generates HTML documentation for a ConfigurableComponent. This class is used
 * to generate documentation for ControllerService and ReportingTask because
 * they have no additional information.
 *
 *
 */
public class HtmlDocumentationWriter implements DocumentationWriter {

    public static final Logger LOGGER = LoggerFactory.getLogger(HtmlDocumentationWriter.class);

    /**
     * The filename where additional user specified information may be stored.
     */
    public static final String ADDITIONAL_DETAILS_HTML = "additionalDetails.html";

    private final ExtensionManager extensionManager;

    public HtmlDocumentationWriter(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public void write(final ConfigurableComponent configurableComponent, final OutputStream streamToWriteTo,
            final boolean includesAdditionalDocumentation) throws IOException {

        try {
            XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(
                    streamToWriteTo, "UTF-8");
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
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream to write to
     * @throws XMLStreamException thrown if there was a problem writing to the
     * stream
     */
    protected void writeHead(final ConfigurableComponent configurableComponent,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("head");
        xmlStreamWriter.writeStartElement("meta");
        xmlStreamWriter.writeAttribute("charset", "utf-8");
        xmlStreamWriter.writeEndElement();
        writeSimpleElement(xmlStreamWriter, "title", getTitle(configurableComponent));

        xmlStreamWriter.writeStartElement("link");
        xmlStreamWriter.writeAttribute("rel", "stylesheet");
        xmlStreamWriter.writeAttribute("href", "../../../../../css/component-usage.css");
        xmlStreamWriter.writeAttribute("type", "text/css");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("script");
        xmlStreamWriter.writeAttribute("type", "text/javascript");
        xmlStreamWriter.writeCharacters("window.onload = function(){if(self==top) { " +
                "document.getElementById('nameHeader').style.display = \"inherit\"; } }" );
        xmlStreamWriter.writeEndElement();

    }

    /**
     * Gets the class name of the component.
     *
     * @param configurableComponent the component to describe
     * @return the class name of the component
     */
    protected String getTitle(final ConfigurableComponent configurableComponent) {
        return configurableComponent.getClass().getSimpleName();
    }

    /**
     * Writes the body section of the documentation, this consists of the
     * component description, the tags, and the PropertyDescriptors.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer
     * @param hasAdditionalDetails whether there are additional details present
     * or not
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML stream
     */
    private void writeBody(final ConfigurableComponent configurableComponent,
            final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails)
            throws XMLStreamException {
        xmlStreamWriter.writeStartElement("body");
        writeHeader(configurableComponent, xmlStreamWriter);
        writeDeprecationWarning(configurableComponent, xmlStreamWriter);
        writeDescription(configurableComponent, xmlStreamWriter, hasAdditionalDetails);
        writeTags(configurableComponent, xmlStreamWriter);
        writeProperties(configurableComponent, xmlStreamWriter);
        writeDynamicProperties(configurableComponent, xmlStreamWriter);
        writeAdditionalBodyInfo(configurableComponent, xmlStreamWriter);
        writeStatefulInfo(configurableComponent, xmlStreamWriter);
        writeRestrictedInfo(configurableComponent, xmlStreamWriter);
        writeInputRequirementInfo(configurableComponent, xmlStreamWriter);
        writeSystemResourceConsiderationInfo(configurableComponent, xmlStreamWriter);
        writeSeeAlso(configurableComponent, xmlStreamWriter);
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Write the header to be displayed when loaded outside an iframe.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeHeader(ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        xmlStreamWriter.writeStartElement("h1");
        xmlStreamWriter.writeAttribute("id", "nameHeader");
        // Style will be overwritten on load if needed
        xmlStreamWriter.writeAttribute("style", "display: none;");
        xmlStreamWriter.writeCharacters(getTitle(configurableComponent));
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Add in the documentation information regarding the component whether it accepts an
     * incoming relationship or not.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeInputRequirementInfo(ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final InputRequirement inputRequirement = configurableComponent.getClass().getAnnotation(InputRequirement.class);

        if(inputRequirement != null) {
            writeSimpleElement(xmlStreamWriter, "h3", "Input requirement: ");
            switch (inputRequirement.value()) {
                case INPUT_FORBIDDEN:
                    xmlStreamWriter.writeCharacters("This component does not allow an incoming relationship.");
                    break;
                case INPUT_ALLOWED:
                    xmlStreamWriter.writeCharacters("This component allows an incoming relationship.");
                    break;
                case INPUT_REQUIRED:
                    xmlStreamWriter.writeCharacters("This component requires an incoming relationship.");
                    break;
                default:
                    xmlStreamWriter.writeCharacters("This component does not have input requirement.");
                    break;
            }
        }
    }

    /**
     * Write the description of the Stateful annotation if provided in this component.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeStatefulInfo(ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final Stateful stateful = configurableComponent.getClass().getAnnotation(Stateful.class);

        writeSimpleElement(xmlStreamWriter, "h3", "State management: ");

        if(stateful != null) {
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "stateful");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Scope");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "td", join(stateful.scopes(), ", "));
            writeSimpleElement(xmlStreamWriter, "td", stateful.description());
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeEndElement();
        } else {
            xmlStreamWriter.writeCharacters("This component does not store state.");
        }
    }

    /**
     * Write the description of the Restricted annotation if provided in this component.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeRestrictedInfo(ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final Restricted restricted = configurableComponent.getClass().getAnnotation(Restricted.class);

        writeSimpleElement(xmlStreamWriter, "h3", "Restricted: ");

        if(restricted != null) {
            final String value = restricted.value();

            if (!StringUtils.isBlank(value)) {
                xmlStreamWriter.writeCharacters(restricted.value());
            }

            final Restriction[] restrictions = restricted.restrictions();
            if (restrictions != null && restrictions.length > 0) {
                xmlStreamWriter.writeStartElement("table");
                xmlStreamWriter.writeAttribute("id", "restrictions");
                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "th", "Required Permission");
                writeSimpleElement(xmlStreamWriter, "th", "Explanation");
                xmlStreamWriter.writeEndElement();

                for (Restriction restriction : restrictions) {
                    xmlStreamWriter.writeStartElement("tr");
                    writeSimpleElement(xmlStreamWriter, "td", restriction.requiredPermission().getPermissionLabel());
                    writeSimpleElement(xmlStreamWriter, "td", restriction.explanation());
                    xmlStreamWriter.writeEndElement();
                }

                xmlStreamWriter.writeEndElement();
            } else {
                xmlStreamWriter.writeCharacters("This component requires access to restricted components regardless of restriction.");
            }
        } else {
            xmlStreamWriter.writeCharacters("This component is not restricted.");
        }
    }

    /**
     * Writes a warning about the deprecation of a component.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML stream
     */
    private void writeDeprecationWarning(final ConfigurableComponent configurableComponent,
                                         final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final DeprecationNotice deprecationNotice = configurableComponent.getClass().getAnnotation(DeprecationNotice.class);
        if (deprecationNotice != null) {
            xmlStreamWriter.writeStartElement("h2");
            xmlStreamWriter.writeCharacters("Deprecation notice: ");
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeStartElement("p");

            xmlStreamWriter.writeCharacters("");
            if (!StringUtils.isEmpty(deprecationNotice.reason())) {
                xmlStreamWriter.writeCharacters(deprecationNotice.reason());
            } else {
                // Write a default note
                xmlStreamWriter.writeCharacters("Please be aware this processor is deprecated and may be removed in " +
                        "the near future.");
            }
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeStartElement("p");
            xmlStreamWriter.writeCharacters("Please consider using one the following alternatives: ");


            Class<? extends ConfigurableComponent>[] componentNames = deprecationNotice.alternatives();
            String[] classNames = deprecationNotice.classNames();

            if (componentNames.length > 0 || classNames.length > 0) {
                // Write alternatives
                iterateAndLinkComponents(xmlStreamWriter, componentNames, classNames, ",", configurableComponent.getClass().getSimpleName());
            } else {
                xmlStreamWriter.writeCharacters("No alternative components suggested.");
            }

            xmlStreamWriter.writeEndElement();
        }
    }

    /**
     * Writes the list of components that may be linked from this component.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeSeeAlso(ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final SeeAlso seeAlso = configurableComponent.getClass().getAnnotation(SeeAlso.class);
        if (seeAlso != null) {
            writeSimpleElement(xmlStreamWriter, "h3", "See Also:");
            xmlStreamWriter.writeStartElement("p");

            Class<? extends ConfigurableComponent>[] componentNames = seeAlso.value();
            String[] classNames = seeAlso.classNames();
            if (componentNames.length > 0 || classNames.length > 0) {
                // Write alternatives
                iterateAndLinkComponents(xmlStreamWriter, componentNames, classNames, ", ", configurableComponent.getClass().getSimpleName());
            } else {
                xmlStreamWriter.writeCharacters("No tags provided.");
            }

            xmlStreamWriter.writeEndElement();
        }
    }

    /**
     * This method may be overridden by sub classes to write additional
     * information to the body of the documentation.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML stream
     */
    protected void writeAdditionalBodyInfo(final ConfigurableComponent configurableComponent,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

    }

    private void writeTags(final ConfigurableComponent configurableComponent,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final Tags tags = configurableComponent.getClass().getAnnotation(Tags.class);
        xmlStreamWriter.writeStartElement("h3");
        xmlStreamWriter.writeCharacters("Tags: ");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeStartElement("p");
        if (tags != null) {
            final String tagString = join(tags.value(), ", ");
            xmlStreamWriter.writeCharacters(tagString);
        } else {
            xmlStreamWriter.writeCharacters("No tags provided.");
        }
        xmlStreamWriter.writeEndElement();
    }

    static String join(final Object[] objects, final String delimiter) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < objects.length; i++) {
            sb.append(objects[i].toString());
            if (i < objects.length - 1) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    /**
     * Writes a description of the configurable component.
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer
     * @param hasAdditionalDetails whether there are additional details
     * available as 'additionalDetails.html'
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML stream
     */
    protected void writeDescription(final ConfigurableComponent configurableComponent,
            final XMLStreamWriter xmlStreamWriter, final boolean hasAdditionalDetails)
            throws XMLStreamException {
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
     * @param configurableComponent the component to describe
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
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the
     * XML Stream
     */
    protected void writeProperties(final ConfigurableComponent configurableComponent,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

        final List<PropertyDescriptor> properties = configurableComponent.getPropertyDescriptors();
        writeSimpleElement(xmlStreamWriter, "h3", "Properties: ");

        if (properties.size() > 0) {
            final boolean containsExpressionLanguage = containsExpressionLanguage(configurableComponent);
            final boolean containsSensitiveProperties = containsSensitiveProperties(configurableComponent);
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
                writeLink(xmlStreamWriter, "NiFi Expression Language", "../../../../../html/expression-language-guide.html");
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
                    String text = "Supports Expression Language: true";
                    final String perFF = " (will be evaluated using flow file attributes and variable registry)";
                    final String registry = " (will be evaluated using variable registry only)";
                    final InputRequirement inputRequirement = configurableComponent.getClass().getAnnotation(InputRequirement.class);

                    switch(property.getExpressionLanguageScope()) {
                        case FLOWFILE_ATTRIBUTES:
                            if(inputRequirement != null && inputRequirement.value().equals(Requirement.INPUT_FORBIDDEN)) {
                                text += registry;
                            } else {
                                text += perFF;
                            }
                            break;
                        case VARIABLE_REGISTRY:
                            text += registry;
                            break;
                        case NONE:
                        default:
                            // in case legacy/deprecated method has been used to specify EL support
                            text += " (undefined scope)";
                            break;
                    }

                    writeSimpleElement(xmlStreamWriter, "strong", text);
                }
                xmlStreamWriter.writeEndElement();

                xmlStreamWriter.writeEndElement();
            }

            xmlStreamWriter.writeEndElement();

        } else {
            writeSimpleElement(xmlStreamWriter, "p", "This component has no required or optional properties.");
        }
    }

    /**
     * Indicates whether or not the component contains at least one sensitive property.
     *
     * @param component the component to interogate
     * @return whether or not the component contains at least one sensitive property.
     */
    private boolean containsSensitiveProperties(final ConfigurableComponent component) {
        for (PropertyDescriptor descriptor : component.getPropertyDescriptors()) {
            if (descriptor.isSensitive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Indicates whether or not the component contains at least one property that supports Expression Language.
     *
     * @param component the component to interrogate
     * @return whether or not the component contains at least one sensitive property.
     */
    private boolean containsExpressionLanguage(final ConfigurableComponent component) {
        for (PropertyDescriptor descriptor : component.getPropertyDescriptors()) {
            if (descriptor.isExpressionLanguageSupported()) {
                return true;
            }
        }
        return false;
    }

    private void writeDynamicProperties(final ConfigurableComponent configurableComponent,
            final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

        final List<DynamicProperty> dynamicProperties = getDynamicProperties(configurableComponent);

        if (dynamicProperties != null && dynamicProperties.size() > 0) {
            writeSimpleElement(xmlStreamWriter, "h3", "Dynamic Properties: ");
            xmlStreamWriter.writeStartElement("p");
            xmlStreamWriter
                    .writeCharacters("Dynamic Properties allow the user to specify both the name and value of a property.");
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "dynamic-properties");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Name");
            writeSimpleElement(xmlStreamWriter, "th", "Value");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();
            for (final DynamicProperty dynamicProperty : dynamicProperties) {
                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "td", dynamicProperty.name(), false, "name");
                writeSimpleElement(xmlStreamWriter, "td", dynamicProperty.value(), false, "value");
                xmlStreamWriter.writeStartElement("td");
                xmlStreamWriter.writeCharacters(dynamicProperty.description());

                xmlStreamWriter.writeEmptyElement("br");
                String text;

                if(dynamicProperty.expressionLanguageScope().equals(ExpressionLanguageScope.NONE)) {
                    if(dynamicProperty.supportsExpressionLanguage()) {
                        text = "Supports Expression Language: true (undefined scope)";
                    } else {
                        text = "Supports Expression Language: false";
                    }
                } else {
                    switch(dynamicProperty.expressionLanguageScope()) {
                        case FLOWFILE_ATTRIBUTES:
                            text = "Supports Expression Language: true (will be evaluated using flow file attributes and variable registry)";
                            break;
                        case VARIABLE_REGISTRY:
                            text = "Supports Expression Language: true (will be evaluated using variable registry only)";
                            break;
                        case NONE:
                        default:
                            text = "Supports Expression Language: false";
                            break;
                    }
                }

                writeSimpleElement(xmlStreamWriter, "strong", text);
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.writeEndElement();
            }

            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeEndElement();
        }
    }

    private List<DynamicProperty> getDynamicProperties(ConfigurableComponent configurableComponent) {
        final List<DynamicProperty> dynamicProperties = new ArrayList<>();
        final DynamicProperties dynProps = configurableComponent.getClass().getAnnotation(DynamicProperties.class);
        if (dynProps != null) {
            for (final DynamicProperty dynProp : dynProps.value()) {
                dynamicProperties.add(dynProp);
            }
        }

        final DynamicProperty dynProp = configurableComponent.getClass().getAnnotation(DynamicProperty.class);
        if (dynProp != null) {
            dynamicProperties.add(dynProp);
        }

        return dynamicProperties;
    }

    private void writeValidValueDescription(XMLStreamWriter xmlStreamWriter, String description)
            throws XMLStreamException {
        xmlStreamWriter.writeCharacters(" ");
        xmlStreamWriter.writeStartElement("img");
        xmlStreamWriter.writeAttribute("src", "../../../../../html/images/iconInfo.png");
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

            final List<Class<? extends ControllerService>> implementationList = lookupControllerServiceImpls(controllerServiceClass);

            // Convert it into an array before proceeding
            Class<? extends ControllerService>[] implementations = implementationList.stream().toArray(Class[]::new);

            xmlStreamWriter.writeEmptyElement("br");
            if (implementations.length > 0) {
                final String title = implementations.length > 1 ? "Implementations: " : "Implementation: ";
                writeSimpleElement(xmlStreamWriter, "strong", title);
                iterateAndLinkComponents(xmlStreamWriter, implementations, null, "<br>", controllerServiceClass.getSimpleName());
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
     * Writes all the system resource considerations for this component
     *
     * @param configurableComponent the component to describe
     * @param xmlStreamWriter the xml stream writer to use
     * @throws XMLStreamException thrown if there was a problem writing the XML
     */
    private void writeSystemResourceConsiderationInfo(ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {

        SystemResourceConsideration[] systemResourceConsiderations = configurableComponent.getClass().getAnnotationsByType(SystemResourceConsideration.class);

        writeSimpleElement(xmlStreamWriter, "h3", "System Resource Considerations:");
        if (systemResourceConsiderations.length > 0) {
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "system-resource-considerations");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Resource");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();
            for (SystemResourceConsideration systemResourceConsideration : systemResourceConsiderations) {
                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "td", systemResourceConsideration.resource().name());
                writeSimpleElement(xmlStreamWriter, "td", systemResourceConsideration.description().trim().isEmpty()
                        ? "Not Specified" : systemResourceConsideration.description());
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndElement();

        } else {
            xmlStreamWriter.writeCharacters("None specified.");
        }
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
        final Set<Class> controllerServices = extensionManager.getExtensions(ControllerService.class);

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
     * Writes a link to another configurable component
     *
     * @param xmlStreamWriter the xml stream writer
     * @param linkedComponents the array of configurable component to link to
     * @param classNames the array of class names in string format to link to
     * @param separator a separator used to split the values (in case more than 1. If the separator is enclosed in
     *                  between "<" and ">" (.e.g "<br>" it is treated as a tag and written to the xmlStreamWriter as an
     *                  empty tag
     * @param sourceContextName the source context/name of the item being linked
     * @throws XMLStreamException thrown if there is a problem writing the XML
     */
    protected void iterateAndLinkComponents(final XMLStreamWriter xmlStreamWriter, final Class<? extends ConfigurableComponent>[] linkedComponents,
            final String[] classNames, final String separator, final String sourceContextName)
            throws XMLStreamException {
        String effectiveSeparator = separator;
        // Treat the the possible separators
        boolean separatorIsElement;

        if (effectiveSeparator.startsWith("<") && effectiveSeparator.endsWith(">")) {
            separatorIsElement = true;
        } else {
            separatorIsElement = false;
        }
        // Whatever the result, strip the possible < and > characters
        effectiveSeparator = effectiveSeparator.replaceAll("\\<([^>]*)>","$1");

        int index = 0;
        for (final Class<? extends ConfigurableComponent> linkedComponent : linkedComponents ) {
            final String linkedComponentName = linkedComponent.getName();
            final List<Bundle> linkedComponentBundles = extensionManager.getBundles(linkedComponentName);
            if (linkedComponentBundles != null && linkedComponentBundles.size() > 0) {
                final Bundle firstLinkedComponentBundle = linkedComponentBundles.get(0);
                final BundleCoordinate coordinate = firstLinkedComponentBundle.getBundleDetails().getCoordinate();

                final String group = coordinate.getGroup();
                final String id = coordinate.getId();
                final String version = coordinate.getVersion();



                if (index != 0) {
                    if (separatorIsElement) {
                        xmlStreamWriter.writeEmptyElement(effectiveSeparator);
                    } else {
                        xmlStreamWriter.writeCharacters(effectiveSeparator);
                    }
                }
                writeLink(xmlStreamWriter, linkedComponent.getSimpleName(), "../../../../../components/" + group + "/" + id + "/" + version + "/" + linkedComponent.getCanonicalName() + "/index.html");

                ++index;
            } else {
                LOGGER.warn("Could not link to {} because no bundles were found for {}", new Object[] {linkedComponentName, sourceContextName});
            }
        }

        if (classNames!= null) {
            for (final String className : classNames) {
                if (index != 0) {
                    if (separatorIsElement) {
                        xmlStreamWriter.writeEmptyElement(effectiveSeparator);
                    } else {
                        xmlStreamWriter.writeCharacters(effectiveSeparator);
                    }
                }

                final List<Bundle> linkedComponentBundles = extensionManager.getBundles(className);

                if (linkedComponentBundles != null && linkedComponentBundles.size() > 0) {
                    final Bundle firstBundle = linkedComponentBundles.get(0);
                    final BundleCoordinate firstCoordinate = firstBundle.getBundleDetails().getCoordinate();

                    final String group = firstCoordinate.getGroup();
                    final String id = firstCoordinate.getId();
                    final String version = firstCoordinate.getVersion();

                    final String link = "../../../../../components/" + group + "/" + id + "/" + version + "/" + className + "/index.html";

                    final int indexOfLastPeriod = className.lastIndexOf(".") + 1;

                    writeLink(xmlStreamWriter, className.substring(indexOfLastPeriod), link);

                    ++index;
                } else {
                    LOGGER.warn("Could not link to {} because no bundles were found for {}", new Object[] {className, sourceContextName});
                }
            }
        }

    }

}
