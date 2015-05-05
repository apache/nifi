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
package org.apache.nifi.controller;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class FlowUnmarshaller {

    /**
     * Interprets the given byte array as an XML document that conforms to the Flow Configuration schema and returns a FlowSnippetDTO representing the flow
     *
     * @param flowContents contents
     * @param encryptor encryptor
     * @return snippet dto
     * @throws NullPointerException if <code>flowContents</code> is null
     * @throws IOException ioe
     * @throws SAXException sax
     * @throws ParserConfigurationException pe
     */
    public static FlowSnippetDTO unmarshal(final byte[] flowContents, final StringEncryptor encryptor) throws IOException, SAXException, ParserConfigurationException {
        if (Objects.requireNonNull(flowContents).length == 0) {
            return new FlowSnippetDTO();
        }

        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder docBuilder = dbf.newDocumentBuilder();

        final Document document = docBuilder.parse(new ByteArrayInputStream(flowContents));
        final FlowSnippetDTO flowDto = new FlowSnippetDTO();

        final NodeList nodeList = document.getElementsByTagName("rootGroup");
        if (nodeList.getLength() == 0) {
            return flowDto;
        }
        if (nodeList.getLength() > 1) {
            throw new IllegalArgumentException("Contents contain multiple rootGroup elements");
        }

        final Set<ProcessGroupDTO> rootGroupSet = new HashSet<>();
        flowDto.setProcessGroups(rootGroupSet);
        rootGroupSet.add(FlowFromDOMFactory.getProcessGroup(null, (Element) nodeList.item(0), encryptor));

        return flowDto;
    }
}
