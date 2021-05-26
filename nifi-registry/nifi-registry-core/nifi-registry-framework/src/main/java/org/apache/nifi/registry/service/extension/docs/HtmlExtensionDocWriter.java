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
package org.apache.nifi.registry.service.extension.docs;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.extension.bundle.BundleInfo;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.manifest.AllowableValue;
import org.apache.nifi.registry.extension.component.manifest.ControllerServiceDefinition;
import org.apache.nifi.registry.extension.component.manifest.DeprecationNotice;
import org.apache.nifi.registry.extension.component.manifest.DynamicProperty;
import org.apache.nifi.registry.extension.component.manifest.ExpressionLanguageScope;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.manifest.InputRequirement;
import org.apache.nifi.registry.extension.component.manifest.Property;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.component.manifest.Restricted;
import org.apache.nifi.registry.extension.component.manifest.Restriction;
import org.apache.nifi.registry.extension.component.manifest.Stateful;
import org.apache.nifi.registry.extension.component.manifest.SystemResourceConsideration;
import org.springframework.stereotype.Service;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.registry.service.extension.docs.DocumentationConstants.CSS_PATH;

@Service
public class HtmlExtensionDocWriter implements ExtensionDocWriter {

    @Override
    public void write(final ExtensionMetadata extensionMetadata, final Extension extension, final OutputStream outputStream) throws IOException {
        try {
            final XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, "UTF-8");
            xmlStreamWriter.writeDTD("<!DOCTYPE html>");
            xmlStreamWriter.writeStartElement("html");
            xmlStreamWriter.writeAttribute("lang", "en");
            writeHead(extensionMetadata, xmlStreamWriter);
            writeBody(extensionMetadata, extension, xmlStreamWriter);
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.close();
            outputStream.flush();
        } catch (XMLStreamException | FactoryConfigurationError e) {
            throw new IOException("Unable to create XMLOutputStream", e);
        }
    }

    private void writeHead(final ExtensionMetadata extensionMetadata, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("head");
        xmlStreamWriter.writeStartElement("meta");
        xmlStreamWriter.writeAttribute("charset", "utf-8");
        xmlStreamWriter.writeEndElement();
        writeSimpleElement(xmlStreamWriter, "title", extensionMetadata.getDisplayName());

        final String componentUsageCss = CSS_PATH + "component-usage.css";
        xmlStreamWriter.writeStartElement("link");
        xmlStreamWriter.writeAttribute("rel", "stylesheet");
        xmlStreamWriter.writeAttribute("href", componentUsageCss);
        xmlStreamWriter.writeAttribute("type", "text/css");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("script");
        xmlStreamWriter.writeAttribute("type", "text/javascript");
        xmlStreamWriter.writeCharacters("window.onload = function(){if(self==top) { " +
                "document.getElementById('nameHeader').style.display = \"inherit\"; } }" );
        xmlStreamWriter.writeEndElement();
    }

    private void writeBody(final ExtensionMetadata extensionMetadata, final Extension extension,
                           final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("body");

        writeHeader(extensionMetadata, extension, xmlStreamWriter);
        writeBundleInfo(extensionMetadata, xmlStreamWriter);
        writeDeprecationWarning(extension, xmlStreamWriter);
        writeDescription(extensionMetadata, extension, xmlStreamWriter);
        writeTags(extension, xmlStreamWriter);
        writeProperties(extension, xmlStreamWriter);
        writeDynamicProperties(extension, xmlStreamWriter);
        writeAdditionalBodyInfo(extension, xmlStreamWriter);
        writeStatefulInfo(extension, xmlStreamWriter);
        writeRestrictedInfo(extension, xmlStreamWriter);
        writeInputRequirementInfo(extension, xmlStreamWriter);
        writeSystemResourceConsiderationInfo(extension, xmlStreamWriter);
        writeProvidedServiceApis(extension, xmlStreamWriter);
        writeSeeAlso(extension, xmlStreamWriter);

        // end body
        xmlStreamWriter.writeEndElement();
    }

    /**
     * This method may be overridden by sub classes to write additional
     * information to the body of the documentation.
     *
     * @param extension the component to describe
     * @param xmlStreamWriter the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the XML stream
     */
    protected void writeAdditionalBodyInfo(final Extension extension, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

    }

    private void writeHeader(final ExtensionMetadata extensionMetadata, final Extension extension,
                             final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        xmlStreamWriter.writeStartElement("h1");
        xmlStreamWriter.writeAttribute("id", "nameHeader");
        xmlStreamWriter.writeAttribute("style", "display: none;");
        xmlStreamWriter.writeCharacters(extensionMetadata.getDisplayName());
        xmlStreamWriter.writeEndElement();
    }

    private void writeBundleInfoString(final ExtensionMetadata extensionMetadata, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final BundleInfo bundleInfo = extensionMetadata.getBundleInfo();
        final String bundleInfoText = bundleInfo.getGroupId() + "-" + bundleInfo.getArtifactId() + "-" + bundleInfo.getVersion();
        xmlStreamWriter.writeStartElement("p");
        xmlStreamWriter.writeStartElement("i");
        xmlStreamWriter.writeCharacters(bundleInfoText);
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeEndElement();
    }

    private void writeBundleInfo(final ExtensionMetadata extensionMetadata, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final BundleInfo bundleInfo = extensionMetadata.getBundleInfo();

        final String extenstionType;
        switch (extensionMetadata.getType()) {
            case PROCESSOR:
                extenstionType = "Processor";
                break;
            case CONTROLLER_SERVICE:
                extenstionType = "Controller Service";
                break;
            case REPORTING_TASK:
                extenstionType = "Reporting Task";
                break;
            default:
                throw new IllegalArgumentException("Unknown extension type: " + extensionMetadata.getType());
        }

        xmlStreamWriter.writeStartElement("table");

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "th", "Extension Info");
        writeSimpleElement(xmlStreamWriter, "th", "Value");
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "td", "Full Name", true, "bundle-info");
        writeSimpleElement(xmlStreamWriter, "td", extensionMetadata.getName());
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "td", "Type", true, "bundle-info");
        writeSimpleElement(xmlStreamWriter, "td", extenstionType);
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "td", "Bundle Group", true, "bundle-info");
        writeSimpleElement(xmlStreamWriter, "td", bundleInfo.getGroupId());
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "td", "Bundle Artifact", true, "bundle-info");
        writeSimpleElement(xmlStreamWriter, "td", bundleInfo.getArtifactId());
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "td", "Bundle Version", true, "bundle-info");
        writeSimpleElement(xmlStreamWriter, "td", bundleInfo.getVersion());
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "td", "Bundle Type", true, "bundle-info");
        writeSimpleElement(xmlStreamWriter, "td", bundleInfo.getBundleType().toString());
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("tr");
        writeSimpleElement(xmlStreamWriter, "td", "System API Version", true, "bundle-info");
        writeSimpleElement(xmlStreamWriter, "td", bundleInfo.getSystemApiVersion());
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeEndElement(); // end table
    }

    private void writeDeprecationWarning(final Extension extension, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final DeprecationNotice deprecationNotice = extension.getDeprecationNotice();
        if (deprecationNotice != null) {
            xmlStreamWriter.writeStartElement("h2");
            xmlStreamWriter.writeCharacters("Deprecation notice: ");
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeStartElement("p");
            xmlStreamWriter.writeCharacters("");
            if (!StringUtils.isEmpty(deprecationNotice.getReason())) {
                xmlStreamWriter.writeCharacters(deprecationNotice.getReason());
            } else {
                xmlStreamWriter.writeCharacters("Please be aware this processor is deprecated and may be removed in the near future.");
            }
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeStartElement("p");
            xmlStreamWriter.writeCharacters("Please consider using one the following alternatives: ");

            final List<String> alternatives = deprecationNotice.getAlternatives();
            if (alternatives != null && alternatives.size() > 0) {
                xmlStreamWriter.writeStartElement("ul");
                for (final String alternative : alternatives) {
                    xmlStreamWriter.writeStartElement("li");
                    xmlStreamWriter.writeCharacters(alternative);
                    xmlStreamWriter.writeEndElement();
                }
                xmlStreamWriter.writeEndElement();
            } else {
                xmlStreamWriter.writeCharacters("No alternative components suggested.");
            }

            xmlStreamWriter.writeEndElement();
        }
    }

    private void writeDescription(final ExtensionMetadata extensionMetadata, final Extension extension, final XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final String description = StringUtils.isBlank(extension.getDescription())
                ? "No description provided." : extension.getDescription();
        writeSimpleElement(xmlStreamWriter, "h2", "Description: ");
        writeSimpleElement(xmlStreamWriter, "p", description);

        if (extensionMetadata.getHasAdditionalDetails()) {
            xmlStreamWriter.writeStartElement("p");
            final BundleInfo bundleInfo = extensionMetadata.getBundleInfo();
            final String bucketName = bundleInfo.getBucketName();
            final String groupId = bundleInfo.getGroupId();
            final String artifactId = bundleInfo.getArtifactId();
            final String version = bundleInfo.getVersion();
            final String extensionName = extensionMetadata.getName();

            final String additionalDetailsPath = "/nifi-registry-api/extension-repository/"
                    + bucketName + "/" + groupId + "/" + artifactId + "/" + version
                    + "/extensions/" + extensionName + "/docs/additional-details";

            writeLink(xmlStreamWriter, "Additional Details...", additionalDetailsPath);
            xmlStreamWriter.writeEndElement();
        }
    }

    private void writeTags(final Extension extension, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<String> tags =  extension.getTags();
        xmlStreamWriter.writeStartElement("h3");
        xmlStreamWriter.writeCharacters("Tags: ");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeStartElement("p");
        if (tags != null) {
            final String tagString =  StringUtils.join(tags, ", ");
            xmlStreamWriter.writeCharacters(tagString);
        } else {
            xmlStreamWriter.writeCharacters("No tags provided.");
        }
        xmlStreamWriter.writeEndElement();
    }

    protected void writeProperties(final Extension extension, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

        final List<Property> properties = extension.getProperties();
        writeSimpleElement(xmlStreamWriter, "h3", "Properties: ");

        if (properties != null && properties.size() > 0) {
            final boolean containsExpressionLanguage = containsExpressionLanguage(extension);
            final boolean containsSensitiveProperties = containsSensitiveProperties(extension);
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
                xmlStreamWriter.writeCharacters("whether a property supports the NiFi Expression Language");
            }
            if (containsSensitiveProperties) {
                xmlStreamWriter.writeCharacters(", and whether a property is considered " + "\"sensitive\", meaning that its value will be encrypted");
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
            for (Property property : properties) {
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
                    final InputRequirement inputRequirement = extension.getInputRequirement();

                    switch(property.getExpressionLanguageScope()) {
                        case FLOWFILE_ATTRIBUTES:
                            if(inputRequirement != null && inputRequirement.equals(InputRequirement.INPUT_FORBIDDEN)) {
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

    private boolean containsExpressionLanguage(final Extension extension) {
        for (Property property : extension.getProperties()) {
            if (property.isExpressionLanguageSupported()) {
                return true;
            }
        }
        return false;
    }

    private boolean containsSensitiveProperties(final Extension extension) {
        for (Property property : extension.getProperties()) {
            if (property.isSensitive()) {
                return true;
            }
        }
        return false;
    }

    protected void writeValidValues(final XMLStreamWriter xmlStreamWriter, final Property property) throws XMLStreamException {
        if (property.getAllowableValues() != null && property.getAllowableValues().size() > 0) {
            xmlStreamWriter.writeStartElement("ul");
            for (AllowableValue value : property.getAllowableValues()) {
                xmlStreamWriter.writeStartElement("li");
                xmlStreamWriter.writeCharacters(value.getDisplayName());

                if (!StringUtils.isBlank(value.getDescription())) {
                    writeValidValueDescription(xmlStreamWriter, value.getDescription());
                }
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndElement();
        } else if (property.getControllerServiceDefinition() != null) {
            final ControllerServiceDefinition serviceDefinition = property.getControllerServiceDefinition();
            final String controllerServiceClass = getSimpleName(serviceDefinition.getClassName());

            final String group = serviceDefinition.getGroupId() == null ? "unknown" : serviceDefinition.getGroupId();
            final String artifact = serviceDefinition.getArtifactId() == null ? "unknown" : serviceDefinition.getArtifactId();
            final String version = serviceDefinition.getVersion() == null ? "unknown" : serviceDefinition.getVersion();

            writeSimpleElement(xmlStreamWriter, "strong", "Controller Service API: ");
            xmlStreamWriter.writeEmptyElement("br");
            xmlStreamWriter.writeCharacters(controllerServiceClass);

            writeValidValueDescription(xmlStreamWriter, group + "-" + artifact + "-" + version);

//            xmlStreamWriter.writeEmptyElement("br");
//            xmlStreamWriter.writeCharacters(group);
//            xmlStreamWriter.writeEmptyElement("br");
//            xmlStreamWriter.writeCharacters(artifact);
//            xmlStreamWriter.writeEmptyElement("br");
//            xmlStreamWriter.writeCharacters(version);
        }
    }

    private String getSimpleName(final String extensionName) {
        int index = extensionName.lastIndexOf('.');
        if (index > 0 && (index < (extensionName.length() - 1))) {
            return extensionName.substring(index + 1);
        } else {
            return extensionName;
        }
    }

    private void writeValidValueDescription(final XMLStreamWriter xmlStreamWriter, final String description) throws XMLStreamException {
        xmlStreamWriter.writeCharacters(" ");
        xmlStreamWriter.writeStartElement("img");
        xmlStreamWriter.writeAttribute("src", "/nifi-registry-docs/images/iconInfo.png");
        xmlStreamWriter.writeAttribute("alt", description);
        xmlStreamWriter.writeAttribute("title", description);
        xmlStreamWriter.writeEndElement();
    }

    private void writeDynamicProperties(final Extension extension, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {

        final List<DynamicProperty> dynamicProperties = extension.getDynamicProperties();

        if (dynamicProperties != null && dynamicProperties.size() > 0) {
            writeSimpleElement(xmlStreamWriter, "h3", "Dynamic Properties: ");
            xmlStreamWriter.writeStartElement("p");
            xmlStreamWriter.writeCharacters("Dynamic Properties allow the user to specify both the name and value of a property.");
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "dynamic-properties");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Name");
            writeSimpleElement(xmlStreamWriter, "th", "Value");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();

            for (final DynamicProperty dynamicProperty : dynamicProperties) {
                final String name = StringUtils.isBlank(dynamicProperty.getName()) ? "Not Specified" : dynamicProperty.getName();
                final String value = StringUtils.isBlank(dynamicProperty.getValue()) ? "Not Specified" : dynamicProperty.getValue();
                final String description = StringUtils.isBlank(dynamicProperty.getDescription()) ? "Not Specified" : dynamicProperty.getDescription();

                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "td", name, false, "name");
                writeSimpleElement(xmlStreamWriter, "td", value, false, "value");
                xmlStreamWriter.writeStartElement("td");
                xmlStreamWriter.writeCharacters(description);
                xmlStreamWriter.writeEmptyElement("br");

                final ExpressionLanguageScope elScope = dynamicProperty.getExpressionLanguageScope() == null
                        ? ExpressionLanguageScope.NONE : dynamicProperty.getExpressionLanguageScope();

                String text;
                if(elScope.equals(ExpressionLanguageScope.NONE)) {
                    if(dynamicProperty.isExpressionLanguageSupported()) {
                        text = "Supports Expression Language: true (undefined scope)";
                    } else {
                        text = "Supports Expression Language: false";
                    }
                } else {
                    switch(elScope) {
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

    private void writeStatefulInfo(final Extension extension, final XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final Stateful stateful = extension.getStateful();
        writeSimpleElement(xmlStreamWriter, "h3", "State management: ");

        if(stateful != null) {
            final List<String> scopes = Optional.ofNullable(stateful.getScopes())
                    .map(List::stream)
                    .orElseGet(Stream::empty)
                    .map(s -> s.toString())
                    .collect(Collectors.toList());

            final String description = StringUtils.isBlank(stateful.getDescription()) ? "Not Specified" : stateful.getDescription();

            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "stateful");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Scope");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "td", StringUtils.join(scopes, ", "));
            writeSimpleElement(xmlStreamWriter, "td", description);
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeEndElement();
        } else {
            xmlStreamWriter.writeCharacters("This component does not store state.");
        }
    }

    private void writeRestrictedInfo(final Extension extension, final XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final Restricted restricted = extension.getRestricted();
        writeSimpleElement(xmlStreamWriter, "h3", "Restricted: ");

        if(restricted != null) {
            final String generalRestrictionExplanation = restricted.getGeneralRestrictionExplanation();
            if (!StringUtils.isBlank(generalRestrictionExplanation)) {
                xmlStreamWriter.writeCharacters(generalRestrictionExplanation);
            }

            final List<Restriction> restrictions = restricted.getRestrictions();
            if (restrictions != null && restrictions.size() > 0) {
                xmlStreamWriter.writeStartElement("table");
                xmlStreamWriter.writeAttribute("id", "restrictions");
                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "th", "Required Permission");
                writeSimpleElement(xmlStreamWriter, "th", "Explanation");
                xmlStreamWriter.writeEndElement();

                for (Restriction restriction : restrictions) {
                    final String permission = StringUtils.isBlank(restriction.getRequiredPermission())
                            ? "Not Specified" : restriction.getRequiredPermission();

                    final String explanation = StringUtils.isBlank(restriction.getExplanation())
                            ? "Not Specified" : restriction.getExplanation();

                    xmlStreamWriter.writeStartElement("tr");
                    writeSimpleElement(xmlStreamWriter, "td", permission);
                    writeSimpleElement(xmlStreamWriter, "td", explanation);
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

    private void writeInputRequirementInfo(final Extension extension, final XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final InputRequirement inputRequirement = extension.getInputRequirement();
        if(inputRequirement != null) {
            writeSimpleElement(xmlStreamWriter, "h3", "Input requirement: ");
            switch (inputRequirement) {
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

    private void writeSystemResourceConsiderationInfo(final Extension extension, final XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {

        List<SystemResourceConsideration> systemResourceConsiderations = extension.getSystemResourceConsiderations();

        writeSimpleElement(xmlStreamWriter, "h3", "System Resource Considerations:");
        if (systemResourceConsiderations != null && systemResourceConsiderations.size() > 0) {
            xmlStreamWriter.writeStartElement("table");
            xmlStreamWriter.writeAttribute("id", "system-resource-considerations");
            xmlStreamWriter.writeStartElement("tr");
            writeSimpleElement(xmlStreamWriter, "th", "Resource");
            writeSimpleElement(xmlStreamWriter, "th", "Description");
            xmlStreamWriter.writeEndElement();

            for (SystemResourceConsideration systemResourceConsideration : systemResourceConsiderations) {
                final String resource = StringUtils.isBlank(systemResourceConsideration.getResource())
                        ? "Not Specified" : systemResourceConsideration.getResource();
                final String description = StringUtils.isBlank(systemResourceConsideration.getDescription())
                        ? "Not Specified" : systemResourceConsideration.getDescription();

                xmlStreamWriter.writeStartElement("tr");
                writeSimpleElement(xmlStreamWriter, "td", resource);
                writeSimpleElement(xmlStreamWriter, "td", description);
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndElement();

        } else {
            xmlStreamWriter.writeCharacters("None specified.");
        }
    }

    private void writeProvidedServiceApis(final Extension extension, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<ProvidedServiceAPI> serviceAPIS = extension.getProvidedServiceAPIs();
        if (serviceAPIS != null && serviceAPIS.size() > 0) {
            writeSimpleElement(xmlStreamWriter, "h3", "Provided Service APIs:");

            xmlStreamWriter.writeStartElement("ul");

            for (final ProvidedServiceAPI serviceAPI : serviceAPIS) {
                final String name = getSimpleName(serviceAPI.getClassName());
                final String bundleInfo = " (" + serviceAPI.getGroupId() + "-" + serviceAPI.getArtifactId() + "-" + serviceAPI.getVersion() + ")";

                xmlStreamWriter.writeStartElement("li");
                xmlStreamWriter.writeCharacters(name);
                xmlStreamWriter.writeStartElement("i");
                xmlStreamWriter.writeCharacters(bundleInfo);
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.writeEndElement();
            }

            xmlStreamWriter.writeEndElement();
        }
    }

    private void writeSeeAlso(final Extension extension, final XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        final List<String> seeAlsos = extension.getSeeAlso();
        if (seeAlsos != null && seeAlsos.size() > 0) {
            writeSimpleElement(xmlStreamWriter, "h3", "See Also:");

            xmlStreamWriter.writeStartElement("ul");
            for (final String seeAlso : seeAlsos) {
                writeSimpleElement(xmlStreamWriter, "li", seeAlso);
            }
            xmlStreamWriter.writeEndElement();
        }
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

}
