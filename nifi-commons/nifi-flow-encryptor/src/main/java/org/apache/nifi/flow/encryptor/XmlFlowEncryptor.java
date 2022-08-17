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
package org.apache.nifi.flow.encryptor;

import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.xml.processing.stream.StandardXMLEventReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLEventReaderProvider;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

public class XmlFlowEncryptor extends AbstractFlowEncryptor {
    private static final XMLEventReaderProvider eventReaderProvider = new StandardXMLEventReaderProvider();

    @Override
    public void processFlow(final InputStream inputStream, final OutputStream outputStream,
                            final PropertyEncryptor inputEncryptor, final PropertyEncryptor outputEncryptor) {
        final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        final XMLEventFactory eventFactory = XMLEventFactory.newInstance();

        try {
            final XMLEventReader reader = eventReaderProvider.getEventReader(new StreamSource(inputStream));
            final XMLEventWriter writer = xmlOutputFactory.createXMLEventWriter(outputStream, StandardCharsets.UTF_8.name());
            while (reader.hasNext()) {
                final XMLEvent event = reader.nextEvent();
                if (event.getEventType() == XMLEvent.CHARACTERS) {
                    final Characters characters = event.asCharacters();
                    final String value = characters.getData();
                    final Matcher matcher = ENCRYPTED_PATTERN.matcher(value);
                    if (matcher.matches()) {
                        final String processedValue = getOutputEncrypted(matcher.group(FIRST_GROUP), inputEncryptor, outputEncryptor);
                        writer.add(eventFactory.createCharacters(processedValue));
                    } else {
                        writer.add(characters);
                    }
                } else if (event.getEventType() == XMLEvent.START_DOCUMENT) {
                    writer.add(event);
                    writer.add(eventFactory.createSpace(System.lineSeparator()));
                } else {
                    writer.add(event);
                }
            }
            writer.flush();
            writer.close();
            reader.close();
            outputStream.close();
            inputStream.close();
        } catch (final XMLStreamException e) {
            throw new RuntimeException("Flow XML Processing Failed", e);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed Processing Flow Configuration", e);
        }
    }
}
