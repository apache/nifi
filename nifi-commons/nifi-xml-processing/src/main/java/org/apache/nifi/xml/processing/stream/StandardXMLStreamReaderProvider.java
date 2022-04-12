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
package org.apache.nifi.xml.processing.stream;

import org.apache.nifi.xml.processing.ProcessingException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import java.util.Objects;

/**
 * Standard implementation of XMLStreamReader provider with secure processing enabled
 */
public class StandardXMLStreamReaderProvider implements XMLStreamReaderProvider {
    /**
     * Get XML Stream Reader with external entities disabled
     *
     * @param streamSource Stream Source for Reader
     * @return Configured XML Stream Reader
     */
    @Override
    public XMLStreamReader getStreamReader(final StreamSource streamSource) {
        Objects.requireNonNull(streamSource, "StreamSource required");

        final XMLInputFactory inputFactory = XMLInputFactory.newFactory();
        inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

        try {
            return inputFactory.createXMLStreamReader(streamSource);
        } catch (final XMLStreamException e) {
            throw new ProcessingException("Reader creation failed", e);
        }
    }
}
