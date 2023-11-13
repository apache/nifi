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
package org.apache.nifi.toolkit.config.transformer;

import org.apache.nifi.properties.ProtectedPropertyContext;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.xml.processing.stream.StandardXMLEventReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLEventReaderProvider;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * File Transformer supporting transformation of XML configuration files containing property elements
 */
public class XmlFileTransformer implements FileTransformer {
    private static final Pattern SENSITIVE_PATTERN = Pattern.compile("Password|Secret");

    private static final QName NAME_ATTRIBUTE = QName.valueOf("name");

    private static final QName ENCRYPTION_ATTRIBUTE = QName.valueOf("encryption");

    private static final String IDENTIFIER_ELEMENT_NAME = "identifier";

    private static final XMLEventReaderProvider readerProvider = new StandardXMLEventReaderProvider();

    private final XMLEventFactory eventFactory = XMLEventFactory.newFactory();

    private final SensitivePropertyProvider inputSensitivePropertyProvider;

    private final SensitivePropertyProviderFactory sensitivePropertyProviderFactory;

    private final SensitivePropertyProvider sensitivePropertyProvider;

    /**
     * XML File Transformer with Sensitive Property Provider Factory and Protection Scheme applied when writing output files
     *
     * @param sensitivePropertyProviderFactory Sensitive Property Provider Factory for output files
     * @param protectionScheme Protection Scheme for output files
     */
    public XmlFileTransformer(
            final SensitivePropertyProvider inputSensitivePropertyProvider,
            final SensitivePropertyProviderFactory sensitivePropertyProviderFactory,
            final ProtectionScheme protectionScheme
    ) {
        this.inputSensitivePropertyProvider = Objects.requireNonNull(inputSensitivePropertyProvider, "Input Sensitive Property Provider required");
        this.sensitivePropertyProviderFactory = Objects.requireNonNull(sensitivePropertyProviderFactory, "Sensitive Property Provider Factory required");
        this.sensitivePropertyProvider = sensitivePropertyProviderFactory.getProvider(Objects.requireNonNull(protectionScheme, "Protection Scheme required"));
    }

    /**
     * Read input file using XMLEventReader and write protected values using XMLEventWriter
     *
     * @param inputPath Input file path to be transformed
     * @param outputPath Output file path containing protected values
     * @throws IOException Thrown on transformation failures
     */
    @Override
    public void transform(final Path inputPath, final Path outputPath) throws IOException {
        Objects.requireNonNull(inputPath, "Input path required");
        Objects.requireNonNull(outputPath, "Output path required");

         try (
                 InputStream inputStream = Files.newInputStream(inputPath);
                 OutputStream outputStream = Files.newOutputStream(outputPath)
         ) {
             final StreamSource streamSource = new StreamSource(inputStream);
             final XMLEventReader eventReader = readerProvider.getEventReader(streamSource);

             final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
             final XMLEventWriter eventWriter = outputFactory.createXMLEventWriter(outputStream);

             try {
                 transform(eventReader, eventWriter);
             } finally {
                 eventReader.close();
                 eventWriter.close();
             }
         } catch (final XMLStreamException e) {
             final String message = String.format("XML processing failed [%s]", inputPath);
             throw new IOException(message, e);
         }
    }

    /**
     * Transform input XML Event Reader to output XML Event Writer
     *
     * @param eventReader XML Event Reader
     * @param eventWriter XML Event Writer
     * @throws XMLStreamException Thrown on XML processing failures
     */
    protected void transform(final XMLEventReader eventReader, final XMLEventWriter eventWriter) throws XMLStreamException {
        String groupIdentifier = null;

        while (eventReader.hasNext()) {
            final XMLEvent event = eventReader.nextEvent();

            if (event.isStartElement()) {
                final StartElement startElement = event.asStartElement();
                final QName startElementName = startElement.getName();
                final String startElementLocalPart = startElementName.getLocalPart();
                if (IDENTIFIER_ELEMENT_NAME.equals(startElementLocalPart)) {
                    final XMLEvent nextEvent = eventReader.nextEvent();
                    if (nextEvent.isCharacters()) {
                        groupIdentifier = nextEvent.asCharacters().getData();
                    }
                    eventWriter.add(startElement);
                    eventWriter.add(nextEvent);
                } else if (isEncryptionRequired(startElement)) {
                    transformStartElement(eventReader, eventWriter, startElement, groupIdentifier);
                } else {
                    eventWriter.add(event);
                }
            } else {
                eventWriter.add(event);
            }
        }
    }

    private boolean isEncryptionSupported(final String propertyName) {
        final boolean encryptionSupported;

        if (propertyName == null || propertyName.isEmpty()) {
            encryptionSupported = false;
        } else {
            encryptionSupported = SENSITIVE_PATTERN.matcher(propertyName).find();
        }

        return encryptionSupported;
    }

    private boolean isEncryptionRequired(final StartElement startElement) {
        final String name = getNameAttribute(startElement);
        return isEncryptionSupported(name);
    }

    private void transformStartElement(
            final XMLEventReader eventReader,
            final XMLEventWriter eventWriter,
            final StartElement startElement,
            final String groupIdentifier
    ) throws XMLStreamException {
        final XMLEvent nextEvent = eventReader.nextEvent();
        if (nextEvent.isCharacters()) {
            final Attribute nameAttribute = startElement.getAttributeByName(NAME_ATTRIBUTE);
            final String propertyName = nameAttribute.getValue();
            final ProtectedPropertyContext propertyContext = sensitivePropertyProviderFactory.getPropertyContext(groupIdentifier, propertyName);

            final StartElement encryptedStartElement = getEncryptedStartElement(startElement);
            eventWriter.add(encryptedStartElement);

            final String data = nextEvent.asCharacters().getData();
            final String inputData;
            final String encryption = getEncryptionAttribute(startElement);
            if (encryption == null) {
                inputData = data;
            } else {
                inputData = inputSensitivePropertyProvider.unprotect(data, propertyContext);
            }

            final String protectedProperty = sensitivePropertyProvider.protect(inputData, propertyContext);
            final Characters characters = eventFactory.createCharacters(protectedProperty);
            eventWriter.add(characters);
        } else {
            eventWriter.add(startElement);
            eventWriter.add(nextEvent);
        }
    }

    private String getNameAttribute(final StartElement startElement) {
        final Attribute attribute = startElement.getAttributeByName(NAME_ATTRIBUTE);
        return attribute == null ? null : attribute.getValue();
    }

    private String getEncryptionAttribute(final StartElement startElement) {
        final Attribute attribute = startElement.getAttributeByName(ENCRYPTION_ATTRIBUTE);
        return attribute == null ? null : attribute.getValue();
    }

    private StartElement getEncryptedStartElement(final StartElement startElement) {
        final String name = getNameAttribute(startElement);
        final Attribute nameAttribute = eventFactory.createAttribute(NAME_ATTRIBUTE, name);
        final Attribute encryptionAttribute = eventFactory.createAttribute(ENCRYPTION_ATTRIBUTE, sensitivePropertyProvider.getIdentifierKey());

        final Iterator<Attribute> attributes = Arrays.asList(encryptionAttribute, nameAttribute).iterator();
        final Iterator<Namespace> namespaces = startElement.getNamespaces();
        return eventFactory.createStartElement(startElement.getName(), attributes, namespaces);
    }
}
