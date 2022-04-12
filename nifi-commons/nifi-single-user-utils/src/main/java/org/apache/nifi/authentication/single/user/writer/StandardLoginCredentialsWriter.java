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
package org.apache.nifi.authentication.single.user.writer;

import org.apache.nifi.authentication.single.user.SingleUserCredentials;
import org.apache.nifi.xml.processing.stream.StandardXMLEventReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLEventReaderProvider;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Iterator;

/**
 * Standard Login Credentials Writer updates Login Identity Providers Single User definition with Login Credentials
 */
public class StandardLoginCredentialsWriter implements LoginCredentialsWriter {
    private static final String CLASS_TAG = "class";

    private static final String PROVIDER_TAG = "provider";

    private static final String PROPERTY_TAG = "property";

    private static final String NAME_ATTRIBUTE = "name";

    private static final String USERNAME_PROPERTY = "Username";

    private static final String PASSWORD_PROPERTY = "Password";

    private final XMLEventFactory eventFactory = XMLEventFactory.newFactory();

    private final File providersFile;

    public StandardLoginCredentialsWriter(final File providersFile) {
        this.providersFile = providersFile;
    }

    @Override
    public void writeLoginCredentials(final SingleUserCredentials singleUserCredentials) {
        final byte[] providers = readProviders();
        try (final InputStream providersInputStream = new ByteArrayInputStream(providers)) {
            writeLoginCredentials(singleUserCredentials, providersInputStream);
        } catch (final IOException e) {
            throw new UncheckedIOException("Writing Login Identity Providers Failed", e);
        } catch (final XMLStreamException e) {
            throw new RuntimeException("Processing Login Identity Providers Failed", e);
        }
    }

    private byte[] readProviders() {
        try {
            return Files.readAllBytes(providersFile.toPath());
        } catch (final IOException e) {
            throw new UncheckedIOException("Reading Login Identity Providers Failed", e);
        }
    }

    private void writeLoginCredentials(final SingleUserCredentials singleUserCredentials, final InputStream inputStream) throws IOException, XMLStreamException {
        try (final OutputStream outputStream = new FileOutputStream(providersFile)) {
            final XMLEventWriter providersWriter = getProvidersWriter(outputStream);
            final XMLEventReader providersReader = getProvidersReader(inputStream);
            updateLoginIdentityProviders(singleUserCredentials, providersReader, providersWriter);
            providersReader.close();
            providersWriter.close();
        }
    }

    private void updateLoginIdentityProviders(final SingleUserCredentials singleUserCredentials,
                                              final XMLEventReader providersReader,
                                              final XMLEventWriter providersWriter) throws XMLStreamException {
        boolean processingSingleUserProvider = false;

        while (providersReader.hasNext()) {
            final XMLEvent event = providersReader.nextEvent();
            providersWriter.add(event);

            if (isStartClass(event)) {
                final XMLEvent nextEvent = providersReader.nextEvent();
                providersWriter.add(nextEvent);
                if (nextEvent.isCharacters()) {
                    final String providerClass = nextEvent.asCharacters().getData();
                    if (singleUserCredentials.getProviderClass().equals(providerClass)) {
                        processingSingleUserProvider = true;
                    }
                }
            } else if (isEndProvider(event)) {
                processingSingleUserProvider = false;
            }

            if (processingSingleUserProvider) {
                if (isStartProperty(event, USERNAME_PROPERTY)) {
                    processProperty(providersReader, providersWriter, singleUserCredentials.getUsername());
                } else if (isStartProperty(event, PASSWORD_PROPERTY)) {
                    processProperty(providersReader, providersWriter, singleUserCredentials.getPassword());
                }
            }
        }
    }

    /**
     * Process Property Value and replace existing Characters when found
     *
     * @param providersReader Providers Reader
     * @param providersWriter Providers Writer
     * @param propertyValue   Property Value to be added
     * @throws XMLStreamException Thrown on XMLEventReader.nextEvent()
     */
    private void processProperty(final XMLEventReader providersReader, final XMLEventWriter providersWriter, final String propertyValue) throws XMLStreamException {
        final XMLEvent nextEvent = providersReader.nextEvent();

        final Characters propertyCharacters = eventFactory.createCharacters(propertyValue);
        providersWriter.add(propertyCharacters);

        if (nextEvent.isEndElement()) {
            providersWriter.add(nextEvent);
        }
    }

    private boolean isStartClass(final XMLEvent event) {
        boolean found = false;

        if (event.isStartElement()) {
            final StartElement startElement = event.asStartElement();
            found = CLASS_TAG.equals(startElement.getName().getLocalPart());
        }

        return found;
    }

    private boolean isStartProperty(final XMLEvent event, final String propertyName) {
        boolean found = false;

        if (event.isStartElement()) {
            final StartElement startElement = event.asStartElement();
            found = PROPERTY_TAG.equals(startElement.getName().getLocalPart()) && isProperty(startElement, propertyName);
        }

        return found;
    }

    private boolean isProperty(final StartElement startElement, final String propertyName) {
        boolean found = false;

        final Iterator<Attribute> attributes = startElement.getAttributes();
        while (attributes.hasNext()) {
            final Attribute attribute = attributes.next();
            if (NAME_ATTRIBUTE.equals(attribute.getName().getLocalPart())) {
                if (propertyName.equals(attribute.getValue())) {
                    found = true;
                    break;
                }
            }
        }

        return found;
    }

    private boolean isEndProvider(final XMLEvent event) {
        boolean found = false;

        if (event.isEndElement()) {
            final EndElement endElement = event.asEndElement();
            found = PROVIDER_TAG.equals(endElement.getName().getLocalPart());
        }

        return found;
    }

    private XMLEventWriter getProvidersWriter(final OutputStream outputStream) throws XMLStreamException {
        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        return outputFactory.createXMLEventWriter(outputStream);
    }

    private XMLEventReader getProvidersReader(final InputStream inputStream) {
        final XMLEventReaderProvider readerProvider = new StandardXMLEventReaderProvider();
        return readerProvider.getEventReader(new StreamSource(inputStream));
    }
}
