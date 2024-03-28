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
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDependency;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceDefinition;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates HTML documentation for a ConfigurableComponent. This class is used
 * to generate documentation for ControllerService, ParameterProvider, and ReportingTask because
 * they have no additional information.
 *
 */
public class HtmlDocumentationWriter extends AbstractHtmlDocumentationWriter<ConfigurableComponent> {

    public static final Logger LOGGER = LoggerFactory.getLogger(HtmlDocumentationWriter.class);

    private final ExtensionManager extensionManager;

    public HtmlDocumentationWriter(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    protected String getTitle(final ConfigurableComponent configurableComponent) {
        return configurableComponent.getClass().getSimpleName();
    }

    @Override
    void writeInputRequirementInfo(final ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final InputRequirement inputRequirement = configurableComponent.getClass().getAnnotation(InputRequirement.class);

        if (inputRequirement != null) {
            writeSimpleElement(xmlStreamWriter, H3, "Input requirement: ");
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

    @Override
    void writeStatefulInfo(final ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final Stateful stateful = configurableComponent.getClass().getAnnotation(Stateful.class);

        writeSimpleElement(xmlStreamWriter, H3, "State management: ");

        if (stateful != null) {
            xmlStreamWriter.writeStartElement(TABLE);
            xmlStreamWriter.writeAttribute(ID, "stateful");
            xmlStreamWriter.writeStartElement(TR);
            writeSimpleElement(xmlStreamWriter, TH, "Scope");
            writeSimpleElement(xmlStreamWriter, TH, "Description");
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeStartElement(TR);
            writeSimpleElement(xmlStreamWriter, TD, join(stateful.scopes()));
            writeSimpleElement(xmlStreamWriter, TD, stateful.description());
            xmlStreamWriter.writeEndElement();

            xmlStreamWriter.writeEndElement();
        } else {
            xmlStreamWriter.writeCharacters("This component does not store state.");
        }
    }

    @Override
    void writeRestrictedInfo(final ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final Restricted restricted = configurableComponent.getClass().getAnnotation(Restricted.class);

        writeSimpleElement(xmlStreamWriter, H3, "Restricted: ");

        if (restricted != null) {
            final String value = restricted.value();

            if (!StringUtils.isBlank(value)) {
                xmlStreamWriter.writeCharacters(restricted.value());
            }

            final Restriction[] restrictions = restricted.restrictions();
            if (restrictions != null && restrictions.length > 0) {
                xmlStreamWriter.writeStartElement(TABLE);
                xmlStreamWriter.writeAttribute(ID, "restrictions");
                xmlStreamWriter.writeStartElement(TR);
                writeSimpleElement(xmlStreamWriter, TH, "Required Permission");
                writeSimpleElement(xmlStreamWriter, TH, "Explanation");
                xmlStreamWriter.writeEndElement();

                for (Restriction restriction : restrictions) {
                    xmlStreamWriter.writeStartElement(TR);
                    writeSimpleElement(xmlStreamWriter, TD, restriction.requiredPermission().getPermissionLabel());
                    writeSimpleElement(xmlStreamWriter, TD, restriction.explanation());
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

    @Override
    void writeDeprecationWarning(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final DeprecationNotice deprecationNotice = configurableComponent.getClass().getAnnotation(DeprecationNotice.class);

        if (deprecationNotice != null) {
            xmlStreamWriter.writeStartElement(H2);
            xmlStreamWriter.writeCharacters("Deprecation notice: ");
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeStartElement(P);

            xmlStreamWriter.writeCharacters("");
            if (!StringUtils.isEmpty(deprecationNotice.reason())) {
                xmlStreamWriter.writeCharacters(deprecationNotice.reason());
            } else {
                // Write a default note
                xmlStreamWriter.writeCharacters("Please be aware this processor is deprecated and may be removed in the near future.");
            }
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeStartElement(P);
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

    @Override
    void writeSeeAlso(final ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final SeeAlso seeAlso = configurableComponent.getClass().getAnnotation(SeeAlso.class);

        if (seeAlso != null) {
            writeSimpleElement(xmlStreamWriter, H3, "See Also:");
            xmlStreamWriter.writeStartElement(P);

            Class<? extends ConfigurableComponent>[] componentNames = seeAlso.value();
            String[] classNames = seeAlso.classNames();
            if (componentNames.length > 0 || classNames.length > 0) {
                // Write alternatives
                iterateAndLinkComponents(xmlStreamWriter, componentNames, classNames, ", ", configurableComponent.getClass().getSimpleName());
            } else {
                xmlStreamWriter.writeCharacters(NO_TAGS);
            }

            xmlStreamWriter.writeEndElement();
        }
    }

    @Override
    void writeTags(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final Tags tags = configurableComponent.getClass().getAnnotation(Tags.class);

        xmlStreamWriter.writeStartElement(H3);
        xmlStreamWriter.writeCharacters("Tags: ");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeStartElement(P);

        if (tags != null) {
            final String tagString = join(tags.value());
            xmlStreamWriter.writeCharacters(tagString);
        } else {
            xmlStreamWriter.writeCharacters(NO_TAGS);
        }

        xmlStreamWriter.writeEndElement();
    }

    static String join(final Object[] objects) {
        return Arrays.stream(objects)
                .map(Object::toString)
                .collect(Collectors.joining(", "));
    }

    @Override
    String getDescription(final ConfigurableComponent configurableComponent) {
        final CapabilityDescription capabilityDescription = configurableComponent.getClass().getAnnotation(CapabilityDescription.class);

        final String description;
        if (capabilityDescription != null) {
            description = capabilityDescription.value();
        } else {
            description = NO_DESCRIPTION;
        }

        return description;
    }

    @Override
    void writeUseCases(final ConfigurableComponent component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final UseCase[] useCases = component.getClass().getAnnotationsByType(UseCase.class);
        if (useCases.length == 0) {
            return;
        }

        writeSimpleElement(xmlStreamWriter, H2, "Example Use Cases:");

        for (final UseCase useCase : useCases) {
            writeSimpleElement(xmlStreamWriter, H3, "Use Case:");
            writeSimpleElement(xmlStreamWriter, P, useCase.description());

            final String notes = useCase.notes();
            if (!StringUtils.isEmpty(notes)) {
                writeSimpleElement(xmlStreamWriter, H4, "Notes:");

                final String[] splits = notes.split("\\n");
                for (final String split : splits) {
                    writeSimpleElement(xmlStreamWriter, P, split);
                }
            }

            final String[] keywords = useCase.keywords();
            if (keywords.length > 0) {
                writeSimpleElement(xmlStreamWriter, H4, "Keywords:");
                xmlStreamWriter.writeCharacters(String.join(", ", keywords));
            }

            final Requirement inputRequirement = useCase.inputRequirement();
            if (inputRequirement != Requirement.INPUT_ALLOWED) {
                writeSimpleElement(xmlStreamWriter, H4, "Input Requirement:");
                xmlStreamWriter.writeCharacters(inputRequirement.toString());
            }

            final String configuration = useCase.configuration();
            writeUseCaseConfiguration(configuration, xmlStreamWriter);

            writeSimpleElement(xmlStreamWriter, BR, null);
        }
    }

    @Override
    void writeMultiComponentUseCases(final ConfigurableComponent component, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final MultiProcessorUseCase[] useCases = component.getClass().getAnnotationsByType(MultiProcessorUseCase.class);
        if (useCases.length == 0) {
            return;
        }

        writeSimpleElement(xmlStreamWriter, H2, "Example Use Cases Involving Other Components:");

        for (final MultiProcessorUseCase useCase : useCases) {
            writeSimpleElement(xmlStreamWriter, H3, "Use Case:");
            writeSimpleElement(xmlStreamWriter, P, useCase.description());

            final String notes = useCase.notes();
            if (!StringUtils.isEmpty(notes)) {
                writeSimpleElement(xmlStreamWriter, H4, "Notes:");

                final String[] splits = notes.split("\\n");
                for (final String split : splits) {
                    writeSimpleElement(xmlStreamWriter, P, split);
                }
            }

            final String[] keywords = useCase.keywords();
            if (keywords.length > 0) {
                writeSimpleElement(xmlStreamWriter, H4, "Keywords:");
                xmlStreamWriter.writeCharacters(String.join(", ", keywords));
            }

            writeSimpleElement(xmlStreamWriter, H4, "Components involved:");
            final ProcessorConfiguration[] processorConfigurations = useCase.configurations();
            for (final ProcessorConfiguration processorConfiguration : processorConfigurations) {
                writeSimpleElement(xmlStreamWriter, STRONG, "Component Type: ");

                final String extensionClassName;
                if (processorConfiguration.processorClassName().isEmpty()) {
                    extensionClassName = processorConfiguration.processorClass().getName();
                } else {
                    extensionClassName = processorConfiguration.processorClassName();
                }

                writeSimpleElement(xmlStreamWriter, SPAN, extensionClassName);

                final String configuration = processorConfiguration.configuration();
                writeUseCaseConfiguration(configuration, xmlStreamWriter);

                writeSimpleElement(xmlStreamWriter, BR, null);
            }


            writeSimpleElement(xmlStreamWriter, BR, null);
        }
    }

    @Override
    void writeProperties(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<PropertyDescriptor> properties = configurableComponent.getPropertyDescriptors();
        writeSimpleElement(xmlStreamWriter, H3, "Properties: ");

        if (!properties.isEmpty()) {
            final boolean containsExpressionLanguage = containsExpressionLanguage(configurableComponent);
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
            writeSimpleElement(xmlStreamWriter, TH, "Allowable Values");
            writeSimpleElement(xmlStreamWriter, TH, "Description");
            xmlStreamWriter.writeEndElement();

            // write the individual properties
            for (PropertyDescriptor property : properties) {
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
                writeSimpleElement(xmlStreamWriter, TD, getDefaultValue(property), "default-value");
                xmlStreamWriter.writeStartElement(TD);
                xmlStreamWriter.writeAttribute(ID, "allowable-values");
                writeValidValues(xmlStreamWriter, property);
                xmlStreamWriter.writeEndElement();
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

                final ResourceDefinition resourceDefinition = property.getResourceDefinition();
                if (resourceDefinition != null) {
                    xmlStreamWriter.writeEmptyElement(BR);
                    xmlStreamWriter.writeEmptyElement(BR);
                    xmlStreamWriter.writeStartElement(STRONG);

                    final ResourceCardinality cardinality = resourceDefinition.getCardinality();
                    final Set<ResourceType> resourceTypes = resourceDefinition.getResourceTypes();
                    if (cardinality == ResourceCardinality.MULTIPLE) {
                        if (resourceTypes.size() == 1) {
                            xmlStreamWriter.writeCharacters("This property expects a comma-separated list of " + resourceTypes.iterator().next() + " resources");
                        } else {
                            xmlStreamWriter.writeCharacters("This property expects a comma-separated list of resources. Each of the resources may be of any of the following types: " +
                                    StringUtils.join(resourceDefinition.getResourceTypes(), ", "));
                        }
                    } else {
                        if (resourceTypes.size() == 1) {
                            xmlStreamWriter.writeCharacters("This property requires exactly one " + resourceTypes.iterator().next() + " to be provided.");
                        } else {
                            xmlStreamWriter.writeCharacters("This property requires exactly one resource to be provided. That resource may be any of the following types: " +
                                    StringUtils.join(resourceDefinition.getResourceTypes(), ", "));
                        }
                    }

                    xmlStreamWriter.writeCharacters(".");
                    xmlStreamWriter.writeEndElement();
                    xmlStreamWriter.writeEmptyElement(BR);
                }

                if (property.isExpressionLanguageSupported()) {
                    xmlStreamWriter.writeEmptyElement(BR);
                    String text = "Supports Expression Language: true";
                    final String perFF = " (will be evaluated using flow file attributes and Environment variables)";
                    final String registry = " (will be evaluated using Environment variables only)";
                    final InputRequirement inputRequirement = configurableComponent.getClass().getAnnotation(InputRequirement.class);

                    switch (property.getExpressionLanguageScope()) {
                        case FLOWFILE_ATTRIBUTES:
                            if (inputRequirement != null && inputRequirement.value().equals(Requirement.INPUT_FORBIDDEN)) {
                                text += registry;
                            } else {
                                text += perFF;
                            }
                            break;
                        case ENVIRONMENT:
                            text += registry;
                            break;
                        case NONE:
                        default:
                            // in case legacy/deprecated method has been used to specify EL support
                            text += " (undefined scope)";
                            break;
                    }

                    writeSimpleElement(xmlStreamWriter, STRONG, text);
                }

                final Set<PropertyDependency> dependencies = property.getDependencies();
                if (!dependencies.isEmpty()) {
                    xmlStreamWriter.writeEmptyElement(BR);
                    xmlStreamWriter.writeEmptyElement(BR);

                    final boolean capitalizeThe;
                    if (dependencies.size() == 1) {
                        writeSimpleElement(xmlStreamWriter, STRONG, "This Property is only considered if ");
                        capitalizeThe = false;
                    } else {
                        writeSimpleElement(xmlStreamWriter, STRONG, "This Property is only considered if all of the following conditions are met:");
                        xmlStreamWriter.writeStartElement(UL);
                        capitalizeThe = true;
                    }

                    for (final PropertyDependency dependency : dependencies) {
                        final Set<String> dependentValues = dependency.getDependentValues();
                        final String prefix = (capitalizeThe ? "The" : "the") + " [" + dependency.getPropertyDisplayName() + "] Property ";
                        String suffix = "";
                        if (dependentValues == null) {
                            suffix = "has a value specified.";
                        } else {
                            PropertyDescriptor dependencyProperty = null;
                            for (PropertyDescriptor prop : properties) {
                                if (prop.getName().equals(dependency.getPropertyName())) {
                                    dependencyProperty = prop;
                                    break;
                                }
                            }
                            if (null == dependencyProperty) {
                                throw new XMLStreamException("No property was found matching the name '" + dependency.getPropertyName() + "'");
                            }
                            if (dependentValues.size() == 1) {
                                final String requiredValue = dependentValues.iterator().next();
                                final List<AllowableValue> allowableValues = dependencyProperty.getAllowableValues();
                                if (allowableValues != null) {
                                    for (AllowableValue av : allowableValues) {
                                        if (requiredValue.equals(av.getValue())) {
                                            suffix = "has a value of \"" + av.getDisplayName() + "\".";
                                            break;
                                        }
                                    }
                                }
                            } else {
                                final StringBuilder sb = new StringBuilder("is set to one of the following values: ");

                                for (final String dependentValue : dependentValues) {
                                    final List<AllowableValue> allowableValues = dependencyProperty.getAllowableValues();
                                    if (allowableValues == null) {
                                        sb.append("[").append(dependentValue).append("], ");
                                    } else {
                                        for (AllowableValue av : allowableValues) {
                                            if (dependentValue.equals(av.getValue())) {
                                                sb.append("[").append(av.getDisplayName()).append("], ");
                                                break;
                                            }
                                        }
                                    }
                                }

                                // Delete the trailing ", "
                                sb.setLength(sb.length() - 2);

                                suffix = sb.toString();
                            }
                        }

                        final String elementName = dependencies.size() > 1 ? LI : STRONG;
                        writeSimpleElement(xmlStreamWriter, elementName, prefix + suffix);
                    }

                    if (dependencies.size() > 1) { // write </ul>
                        xmlStreamWriter.writeEndElement();
                    }
                }

                xmlStreamWriter.writeEndElement();

                xmlStreamWriter.writeEndElement();
            }

            xmlStreamWriter.writeEndElement();

        } else {
            writeSimpleElement(xmlStreamWriter, P, NO_PROPERTIES);
        }
    }

    private String getDefaultValue(final PropertyDescriptor propertyDescriptor) {
        final String defaultValue;

        final String descriptorDefaultValue = propertyDescriptor.getDefaultValue();

        final List<AllowableValue> allowableValues = propertyDescriptor.getAllowableValues();
        if (allowableValues != null) {
            defaultValue = allowableValues.stream()
                    .filter(allowableValue -> allowableValue.getValue().equals(descriptorDefaultValue))
                    .findFirst()
                    .map(AllowableValue::getDisplayName)
                    .orElse(descriptorDefaultValue);
        } else {
            defaultValue = descriptorDefaultValue;
        }

        return defaultValue;
    }

    /**
     * Indicates whether the component contains at least one property that supports Expression Language.
     *
     * @param component the component to interrogate
     * @return whether the component contains at least one sensitive property.
     */
    private boolean containsExpressionLanguage(final ConfigurableComponent component) {
        for (PropertyDescriptor descriptor : component.getPropertyDescriptors()) {
            if (descriptor.isExpressionLanguageSupported()) {
                return true;
            }
        }
        return false;
    }

    @Override
    void writeDynamicProperties(final ConfigurableComponent configurableComponent, final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        final List<DynamicProperty> dynamicProperties = getDynamicProperties(configurableComponent);

        if (!dynamicProperties.isEmpty()) {
            writeSimpleElement(xmlStreamWriter, H3, "Dynamic Properties: ");

            writeSupportsSensitiveDynamicProperties(configurableComponent, xmlStreamWriter);

            xmlStreamWriter.writeStartElement(P);
            xmlStreamWriter.writeCharacters("Dynamic Properties allow the user to specify both the name and value of a property.");
            xmlStreamWriter.writeStartElement(TABLE);
            xmlStreamWriter.writeAttribute(ID, "dynamic-properties");
            xmlStreamWriter.writeStartElement(TR);
            writeSimpleElement(xmlStreamWriter, TH, "Name");
            writeSimpleElement(xmlStreamWriter, TH, "Value");
            writeSimpleElement(xmlStreamWriter, TH, "Description");
            xmlStreamWriter.writeEndElement();
            for (final DynamicProperty dynamicProperty : dynamicProperties) {
                xmlStreamWriter.writeStartElement(TR);
                writeSimpleElement(xmlStreamWriter, TD, dynamicProperty.name(), "name");
                writeSimpleElement(xmlStreamWriter, TD, dynamicProperty.value(), "value");
                xmlStreamWriter.writeStartElement(TD);
                xmlStreamWriter.writeCharacters(dynamicProperty.description());

                xmlStreamWriter.writeEmptyElement(BR);

                final String text = switch (dynamicProperty.expressionLanguageScope()) {
                    case FLOWFILE_ATTRIBUTES ->
                            "Supports Expression Language: true (will be evaluated using flow file attributes and Environment variables)";
                    case ENVIRONMENT ->
                            "Supports Expression Language: true (will be evaluated using Environment variables only)";
                    default -> "Supports Expression Language: false";
                };

                writeSimpleElement(xmlStreamWriter, STRONG, text);
                xmlStreamWriter.writeEndElement();
                xmlStreamWriter.writeEndElement();
            }

            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeEndElement();
        }
    }

    private void writeSupportsSensitiveDynamicProperties(final ConfigurableComponent configurableComponent, final XMLStreamWriter writer) throws XMLStreamException {
        final boolean supportsSensitiveDynamicProperties = configurableComponent.getClass().isAnnotationPresent(SupportsSensitiveDynamicProperties.class);
        final String sensitiveDynamicPropertiesLabel = supportsSensitiveDynamicProperties ? "Yes" : "No";

        writer.writeStartElement(P);

        writer.writeCharacters("Supports Sensitive Dynamic Properties: ");
        writeSimpleElement(writer, STRONG, sensitiveDynamicPropertiesLabel);

        writer.writeEndElement();
    }

    private List<DynamicProperty> getDynamicProperties(final ConfigurableComponent configurableComponent) {
        final List<DynamicProperty> dynamicProperties = new ArrayList<>();
        final DynamicProperties dynProps = configurableComponent.getClass().getAnnotation(DynamicProperties.class);
        if (dynProps != null) {
            Collections.addAll(dynamicProperties, dynProps.value());
        }

        final DynamicProperty dynProp = configurableComponent.getClass().getAnnotation(DynamicProperty.class);
        if (dynProp != null) {
            dynamicProperties.add(dynProp);
        }

        return dynamicProperties;
    }

    private void writeValidValueDescription(XMLStreamWriter xmlStreamWriter, String description) throws XMLStreamException {
        xmlStreamWriter.writeCharacters(" ");
        xmlStreamWriter.writeStartElement("img");
        xmlStreamWriter.writeAttribute("src", "../../../../../html/images/iconInfo.png");
        xmlStreamWriter.writeAttribute("alt", description);
        xmlStreamWriter.writeAttribute("title", description);
        xmlStreamWriter.writeEndElement();
    }

    /**
     * Interrogates a PropertyDescriptor to get a list of AllowableValues, if there are none, nothing is written to the stream.
     *
     * @param xmlStreamWriter the stream writer to use
     * @param property        the property to describe
     * @throws XMLStreamException thrown if there was a problem writing to the XML Stream
     */
    protected void writeValidValues(XMLStreamWriter xmlStreamWriter, PropertyDescriptor property) throws XMLStreamException {
        if (property.getAllowableValues() != null && !property.getAllowableValues().isEmpty()) {
            xmlStreamWriter.writeStartElement(UL);
            for (AllowableValue value : property.getAllowableValues()) {
                xmlStreamWriter.writeStartElement(LI);
                xmlStreamWriter.writeCharacters(value.getDisplayName());

                if (value.getDescription() != null) {
                    writeValidValueDescription(xmlStreamWriter, value.getDescription());
                }
                xmlStreamWriter.writeEndElement();

            }
            xmlStreamWriter.writeEndElement();
        } else if (property.getControllerServiceDefinition() != null) {
            Class<? extends ControllerService> controllerServiceClass = property.getControllerServiceDefinition();

            writeSimpleElement(xmlStreamWriter, STRONG, "Controller Service API: ");
            xmlStreamWriter.writeEmptyElement(BR);
            xmlStreamWriter.writeCharacters(controllerServiceClass.getSimpleName());

            final Class<? extends ControllerService>[] serviceImplementations = lookupControllerServiceImpls(controllerServiceClass);

            xmlStreamWriter.writeEmptyElement(BR);
            if (serviceImplementations.length > 0) {
                final String title = serviceImplementations.length > 1 ? "Implementations: " : "Implementation: ";
                writeSimpleElement(xmlStreamWriter, STRONG, title);
                iterateAndLinkComponents(xmlStreamWriter, serviceImplementations, null, "<br>", controllerServiceClass.getSimpleName());
            } else {
                xmlStreamWriter.writeCharacters("No implementations found.");
            }
        }
    }

    @Override
    void writeSystemResourceConsiderationInfo(final ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
        SystemResourceConsideration[] systemResourceConsiderations = configurableComponent.getClass().getAnnotationsByType(SystemResourceConsideration.class);

        writeSimpleElement(xmlStreamWriter, H3, "System Resource Considerations:");
        if (systemResourceConsiderations.length > 0) {
            xmlStreamWriter.writeStartElement(TABLE);
            xmlStreamWriter.writeAttribute(ID, "system-resource-considerations");
            xmlStreamWriter.writeStartElement(TR);
            writeSimpleElement(xmlStreamWriter, TH, "Resource");
            writeSimpleElement(xmlStreamWriter, TH, "Description");
            xmlStreamWriter.writeEndElement();
            for (SystemResourceConsideration systemResourceConsideration : systemResourceConsiderations) {
                xmlStreamWriter.writeStartElement(TR);
                writeSimpleElement(xmlStreamWriter, TD, systemResourceConsideration.resource().name());
                writeSimpleElement(xmlStreamWriter, TD, systemResourceConsideration.description().trim().isEmpty()
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
     * @return an array of controller services that implement the controller service API
     */
    @SuppressWarnings("unchecked")
    private Class<? extends ControllerService>[] lookupControllerServiceImpls(final Class<? extends ControllerService> parent) {
        final List<Class<? extends ControllerService>> implementations = new ArrayList<>();

        // first get all ControllerService implementations
        final Set<ExtensionDefinition> controllerServices = extensionManager.getExtensions(ControllerService.class);

        // then iterate over all controller services looking for any that is a child of the parent
        // ControllerService API that was passed in as a parameter
        for (final ExtensionDefinition extensionDefinition : controllerServices) {
            final Class<? extends ControllerService> controllerServiceClass = (Class<? extends ControllerService>) extensionManager.getClass(extensionDefinition);
            if (parent.isAssignableFrom(controllerServiceClass)) {
                implementations.add(controllerServiceClass);
            }
        }

        return implementations.toArray(new Class[]{});
    }

    /**
     * Writes a link to another configurable component
     *
     * @param xmlStreamWriter   the xml stream writer
     * @param linkedComponents  the array of configurable component to link to
     * @param classNames        the array of class names in string format to link to
     * @param separator         a separator used to split the values (in case more than 1. If the separator is enclosed in
     *                          between "<" and ">" (.e.g "<br>" it is treated as a tag and written to the xmlStreamWriter as an empty tag
     * @param sourceContextName the source context/name of the item being linked
     * @throws XMLStreamException thrown if there is a problem writing the XML
     */
    protected void iterateAndLinkComponents(final XMLStreamWriter xmlStreamWriter, final Class<? extends ConfigurableComponent>[] linkedComponents,
                                            final String[] classNames, final String separator, final String sourceContextName) throws XMLStreamException {
        String effectiveSeparator = separator;
        // Treat the the possible separators
        final boolean separatorIsElement = effectiveSeparator.startsWith("<") && effectiveSeparator.endsWith(">");
        // Whatever the result, strip the possible < and > characters
        effectiveSeparator = effectiveSeparator.replaceAll("<([^>]*)>", "$1");

        int index = 0;
        for (final Class<? extends ConfigurableComponent> linkedComponent : linkedComponents) {
            final String linkedComponentName = linkedComponent.getName();
            final List<Bundle> linkedComponentBundles = extensionManager.getBundles(linkedComponentName);
            if (linkedComponentBundles != null && !linkedComponentBundles.isEmpty()) {
                final Bundle firstLinkedComponentBundle = linkedComponentBundles.getFirst();
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
                LOGGER.warn("Could not link to {} because no bundles were found for {}", linkedComponentName, sourceContextName);
            }
        }

        if (classNames != null) {
            for (final String className : classNames) {
                if (index != 0) {
                    if (separatorIsElement) {
                        xmlStreamWriter.writeEmptyElement(effectiveSeparator);
                    } else {
                        xmlStreamWriter.writeCharacters(effectiveSeparator);
                    }
                }

                final List<Bundle> linkedComponentBundles = extensionManager.getBundles(className);

                if (linkedComponentBundles != null && !linkedComponentBundles.isEmpty()) {
                    final Bundle firstBundle = linkedComponentBundles.getFirst();
                    final BundleCoordinate firstCoordinate = firstBundle.getBundleDetails().getCoordinate();

                    final String group = firstCoordinate.getGroup();
                    final String id = firstCoordinate.getId();
                    final String version = firstCoordinate.getVersion();

                    final String link = "../../../../../components/" + group + "/" + id + "/" + version + "/" + className + "/index.html";

                    final int indexOfLastPeriod = className.lastIndexOf(".") + 1;

                    writeLink(xmlStreamWriter, className.substring(indexOfLastPeriod), link);

                    ++index;
                } else {
                    LOGGER.warn("Could not link to {} because no bundles were found for {}", className, sourceContextName);
                }
            }
        }

    }

}
