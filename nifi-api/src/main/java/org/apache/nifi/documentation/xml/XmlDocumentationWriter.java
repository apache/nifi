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
package org.apache.nifi.documentation.xml;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.documentation.AbstractDocumentationWriter;
import org.apache.nifi.documentation.ExtensionType;
import org.apache.nifi.documentation.ProvidedServiceAPI;
import org.apache.nifi.processor.Relationship;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * XML-based implementation of DocumentationWriter
 *
 * Please note that while this class lives within the nifi-api, it is provided primarily as a means for documentation components within
 * the NiFi NAR Maven Plugin. Its home is the nifi-api, however, because the API is needed in order to extract the relevant information and
 * the NAR Maven Plugin cannot have a direct dependency on nifi-api (doing so would cause a circular dependency). By having this homed within
 * the nifi-api, the Maven plugin is able to discover the class dynamically and invoke the one or two methods necessary to create the documentation.
 *
 * This is a new capability in 1.9.0 in preparation for the Extension Registry and therefore, you should
 * <b>NOTE WELL:</b> At this time, while this class is part of nifi-api, it is still evolving and may change in a non-backward-compatible manner or even be
 * removed from one incremental release to the next. Use at your own risk!
 */
public class XmlDocumentationWriter extends AbstractDocumentationWriter {
    private final XMLStreamWriter writer;

    public XmlDocumentationWriter(final OutputStream out) throws XMLStreamException {
        this.writer = XMLOutputFactory.newInstance().createXMLStreamWriter(out, "UTF-8");
    }

    public XmlDocumentationWriter(final XMLStreamWriter writer) {
        this.writer = writer;
    }

    @Override
    protected void writeHeader(final ConfigurableComponent component) throws IOException {
        writeStartElement("extension");
    }

    @Override
    protected void writeExtensionName(final String extensionName) throws IOException {
        writeTextElement("name", extensionName);
    }

    @Override
    protected void writeExtensionType(final ExtensionType extensionType) throws IOException {
        writeTextElement("type", extensionType.name());
    }

    @Override
    protected void writeDeprecationNotice(final DeprecationNotice deprecationNotice) throws IOException {
        if (deprecationNotice == null) {
            writeEmptyElement("deprecationNotice");
            return;
        }

        final Class[] classes = deprecationNotice.alternatives();
        final String[] classNames = deprecationNotice.classNames();

        final Set<String> alternatives = new LinkedHashSet<>();
        if (classes != null) {
            for (final Class alternativeClass : classes) {
                alternatives.add(alternativeClass.getName());
            }
        }

        if (classNames != null) {
            Collections.addAll(alternatives, classNames);
        }

        writeDeprecationNotice(deprecationNotice.reason(), alternatives);
    }

    private void writeDeprecationNotice(final String reason, final Set<String> alternatives) throws IOException {
        writeStartElement("deprecationNotice");

        writeTextElement("reason", reason);
        writeTextArray("alternatives", "alternative", alternatives);

        writeEndElement();
    }


    @Override
    protected void writeDescription(final String description) throws IOException {
        writeTextElement("description", description);
    }

    @Override
    protected void writeTags(final List<String> tags) throws IOException {
        writeTextArray("tags", "tag", tags);
    }

    @Override
    protected void writeProperties(final List<PropertyDescriptor> properties) throws IOException {
        writeArray("properties", properties, this::writeProperty);
    }

    private void writeProperty(final PropertyDescriptor property) throws IOException {
        writeStartElement("property");

        writeTextElement("name", property.getName());
        writeTextElement("displayName", property.getDisplayName());
        writeTextElement("description", property.getDescription());
        writeTextElement("defaultValue", property.getDefaultValue());
        writeTextElement("controllerServiceDefinition", property.getControllerServiceDefinition() == null ? null : property.getControllerServiceDefinition().getName());
        writeTextArray("allowableValues", "allowableValue", property.getAllowableValues(), AllowableValue::getDisplayName);
        writeBooleanElement("required", property.isRequired());
        writeBooleanElement("sensitive", property.isSensitive());
        writeBooleanElement("expressionLanguageSupported", property.isExpressionLanguageSupported());
        writeTextElement("expressionLanguageScope", property.getExpressionLanguageScope() == null ? null : property.getExpressionLanguageScope().name());
        writeBooleanElement("dynamicallyModifiesClasspath", property.isDynamicClasspathModifier());
        writeBooleanElement("dynamic", property.isDynamic());

        writeEndElement();
    }

    @Override
    protected void writeDynamicProperties(final List<DynamicProperty> dynamicProperties) throws IOException {
        writeArray("dynamicProperty", dynamicProperties, this::writeDynamicProperty);
    }

    private void writeDynamicProperty(final DynamicProperty property) throws IOException {
        writeStartElement("dynamicProperty");

        writeTextElement("name", property.name());
        writeTextElement("value", property.value());
        writeTextElement("description", property.description());
        writeBooleanElement("expressionLanguageSupported", property.supportsExpressionLanguage());
        writeTextElement("expressionLanguageScope", property.expressionLanguageScope() == null ? null : property.expressionLanguageScope().name());

        writeEndElement();
    }

    @Override
    protected void writeStatefulInfo(final Stateful stateful) throws IOException {
        writeStartElement("stateful");

        if (stateful != null) {
            writeTextElement("description", stateful.description());
            writeArray("scopes", Arrays.asList(stateful.scopes()), scope -> writeTextElement("scope", scope.name()));
        }

        writeEndElement();
    }

    @Override
    protected void writeRestrictedInfo(final Restricted restricted) throws IOException {
        writeStartElement("restricted");

        if (restricted != null) {
            writeTextElement("generalRestrictionExplanation", restricted.value());

            final Restriction[] restrictions = restricted.restrictions();
            if (restrictions != null) {
                writeArray("restrictions", Arrays.asList(restrictions), this::writeRestriction);
            }
        }

        writeEndElement();
    }

    private void writeRestriction(final Restriction restriction) throws IOException {
        writeStartElement("restriction");

        final RequiredPermission permission = restriction.requiredPermission();
        final String label = permission == null ? null : permission.getPermissionLabel();
        writeTextElement("requiredPermission", label);
        writeTextElement("explanation", restriction.explanation());

        writeEndElement();
    }

    @Override
    protected void writeInputRequirementInfo(final InputRequirement.Requirement requirement) throws IOException {
        writeTextElement("inputRequirement", requirement == null ? null : requirement.name());
    }

    @Override
    protected void writeSystemResourceConsiderationInfo(final List<SystemResourceConsideration> considerations) throws IOException {
        writeArray("systemResourceConsiderations", considerations, this::writeSystemResourceConsideration);
    }

    private void writeSystemResourceConsideration(final SystemResourceConsideration consideration) throws IOException {
        writeStartElement("consideration");

        writeTextElement("resource", consideration.resource() == null ? null : consideration.resource().name());
        writeTextElement("description", consideration.description());

        writeEndElement();
    }

    @Override
    protected void writeSeeAlso(final SeeAlso seeAlso) throws IOException {
        if (seeAlso == null) {
            writeEmptyElement("seeAlso");
            return;
        }

        final Class[] classes = seeAlso.value();
        final String[] classNames = seeAlso.classNames();

        final Set<String> toSee = new LinkedHashSet<>();
        if (classes != null) {
            for (final Class classToSee : classes) {
                toSee.add(classToSee.getName());
            }
        }

        if (classNames != null) {
            Collections.addAll(toSee, classNames);
        }

        writeTextArray("seeAlso", "see", toSee);
    }

    @Override
    protected void writeRelationships(final Set<Relationship> relationships) throws IOException {
        writeArray("relationships", relationships,rel -> {
            writeStartElement("relationship");

            writeTextElement("name", rel.getName());
            writeTextElement("description", rel.getDescription());
            writeBooleanElement("autoTerminated", rel.isAutoTerminated());

            writeEndElement();
        } );
    }

    @Override
    protected void writeDynamicRelationship(final DynamicRelationship dynamicRelationship) throws IOException {
        writeStartElement("dynamicRelationship");

        if (dynamicRelationship != null) {
            writeTextElement("name", dynamicRelationship.name());
            writeTextElement("description", dynamicRelationship.description());
        }

        writeEndElement();
    }

    @Override
    protected void writeReadsAttributes(final List<ReadsAttribute> attributes) throws IOException {
        writeArray("readsAttributes", attributes, this::writeReadsAttribute);
    }

    private void writeReadsAttribute(final ReadsAttribute attribute) throws IOException {
        writeStartElement("attribute");
        writeTextElement("name", attribute.attribute());
        writeTextElement("description", attribute.description());
        writeEndElement();
    }

    @Override
    protected void writeWritesAttributes(final List<WritesAttribute> attributes) throws IOException {
        writeArray("writesAttributes", attributes, this::writeWritesAttribute);
    }

    private void writeWritesAttribute(final WritesAttribute attribute) throws IOException {
        writeStartElement("attribute");
        writeTextElement("name", attribute.attribute());
        writeTextElement("description", attribute.description());
        writeEndElement();
    }

    @Override
    protected void writeFooter(final ConfigurableComponent component) throws IOException {
        writeEndElement();
    }

    @Override
    protected void writeProvidedServices(final Collection<ProvidedServiceAPI> providedServices) throws IOException {
        writeStartElement("providedServiceAPIs");
        writeArray("service", providedServices, this::writeProvidedService);
        writeEndElement();
    }

    private void writeProvidedService(final ProvidedServiceAPI service) throws IOException {
        writeTextElement("className",service.getClassName());
        writeTextElement("groupId",service.getGroupId());
        writeTextElement("artifactId",service.getArtifactId());
        writeTextElement("version",service.getVersion());
    }

    private <T> void writeArray(final String tagName, final Collection<T> values, final ElementWriter<T> writer) throws IOException {
        writeStartElement(tagName);

        if (values != null) {
            for (final T value : values) {
                writer.write(value);
            }
        }

        writeEndElement();
    }


    private void writeTextArray(final String outerTagName, final String elementTagName, final Collection<String> values) throws IOException {
        writeTextArray(outerTagName, elementTagName, values, String::toString);
    }

    private <T> void writeTextArray(final String outerTagName, final String elementTagName, final Collection<T> values, final Function<T, String> transform) throws IOException {
        writeStartElement(outerTagName);

        if (values != null) {
            for (final T value : values) {
                writeStartElement(elementTagName);
                if (value != null) {
                    writeText(transform.apply(value));
                }
                writeEndElement();
            }
        }

        writeEndElement();
    }

    private void writeText(final String text) throws IOException {
        if (text == null) {
            return;
        }

        try {
            writer.writeCharacters(text);
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private void writeEmptyElement(final String elementName) throws IOException {
        try {
            writer.writeEmptyElement(elementName);
        } catch (final XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private void writeStartElement(final String elementName) throws IOException {
        try {
            writer.writeStartElement(elementName);
        } catch (final XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private void writeEndElement() throws IOException {
        try {
            writer.writeEndElement();;
        } catch (final XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private void writeTextElement(final String name, final String text) throws IOException {
        writeStartElement(name);
        writeText(text);
        writeEndElement();
    }

    private void writeBooleanElement(final String name, final boolean value) throws IOException {
        writeTextElement(name, String.valueOf(value));
    }

    private interface ElementWriter<T> {
        void write(T value) throws IOException;
    }
}
