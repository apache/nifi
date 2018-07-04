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

package org.apache.nifi.processors.evtx.parser.bxml.value;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.apache.nifi.processors.evtx.parser.BinaryReader;
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.bxml.BxmlNode;

/**
 * Node representing an array of wstring values
 */
public class WStringArrayTypeNode extends VariantTypeNode {
    public static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newFactory();
    private final String value;

    public WStringArrayTypeNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent, int length) throws IOException {
        super(binaryReader, chunkHeader, parent, length);
        String raw;
        if (length >= 0) {
            raw = binaryReader.readWString(length / 2);
        } else {
            int binaryLength = binaryReader.readWord();
            raw = binaryReader.readWString(binaryLength / 2);
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            XMLStreamWriter xmlStreamWriter = XML_OUTPUT_FACTORY.createXMLStreamWriter(stream, "UTF-8");
            for (String s : raw.split("\u0000")) {
                xmlStreamWriter.writeStartElement("string");
                try {
                    xmlStreamWriter.writeCharacters(s);
                } finally {
                    xmlStreamWriter.writeEndElement();
                }
            }
            xmlStreamWriter.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
        value = stream.toString("UTF-8");
    }

    @Override
    public String getValue() {
        return value;
    }
}
