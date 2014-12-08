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
package org.apache.nifi.processors.monitor;
/*
 * NOTE: rule is synonymous with threshold
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processors.monitor.MonitorThreshold.CompareRecord;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class ThresholdsParser {

    private final ProcessorLog logger;

    public ThresholdsParser(ProcessorLog logger) {
        this.logger = logger;
    }

    private CompareRecord parseThresholds(final String attributeName, final Node valueNode) {
        final NamedNodeMap attributes = valueNode.getAttributes();
        final long byteLimit = Long.parseLong(attributes.getNamedItem("size").getTextContent());
        final int filesLimit = Integer.parseInt(attributes.getNamedItem("count").getTextContent());

        String value = null;
        if (valueNode.getNodeName().equals("noMatchRule")) {
            value = MonitorThreshold.DEFAULT_THRESHOLDS_KEY;
        } else {
            value = attributes.getNamedItem("id").getTextContent();
        }

        if (value == null) {
            throw new IllegalArgumentException("Thresholds for " + attributeName + ", size=" + byteLimit + ", count=" + filesLimit + " has no ID");
        }

        final CompareRecord thresholdsState = new CompareRecord(attributeName, value, filesLimit, byteLimit);
        return thresholdsState;
    }

    public Map<String, Map<String, CompareRecord>> readThresholds(String values)
            throws ParserConfigurationException, SAXException, IOException {
        final Map<String, Map<String, CompareRecord>> thresholdsMap = new HashMap<>();

        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder db = factory.newDocumentBuilder();
        final Document doc = db.parse(new InputSource(new ByteArrayInputStream(values.getBytes())));
        final Element configurationElement = doc.getDocumentElement();

        final NodeList secondLevelElementList = configurationElement.getChildNodes();

        if (secondLevelElementList.getLength() == 0) {
            logger.warn("Trying to read thresholds, but no thresholds were parsed.");
        }

        // Loop through the the list of FlowFile attributes (i.e., <flowFileAttribute> elements) and their list of values with limits
        for (int i = 0; i < secondLevelElementList.getLength(); i++) {
            final Node element = secondLevelElementList.item(i);
            final String localName = element.getNodeName();
            if (localName != null && localName.equalsIgnoreCase("flowFileAttribute")) {
                if (element.getNodeType() != Node.ELEMENT_NODE) {
                    logger.debug("Encountered non-ELEMENT_NODE while parsing thresholds: " + element.getNodeName());
                    continue;
                }

                final String attributeName = element.getAttributes().getNamedItem("attributeName").getNodeValue();
                final NodeList children = element.getChildNodes();

                CompareRecord noMatchRule = null;
                List<CompareRecord> thresholds = new ArrayList<>();
                for (int j = 0; j < children.getLength(); j++) {
                    final Node valueNode = children.item(j);
                    if (valueNode == null || valueNode.getNodeType() != Node.ELEMENT_NODE) {
                        continue;
                    }
                    final String nodeName = valueNode.getNodeName();
                    if (nodeName.equalsIgnoreCase("noMatchRule")) {
                        noMatchRule = parseThresholds(attributeName, valueNode);
                    } else if (nodeName.equalsIgnoreCase("rule")) {
                        thresholds.add(parseThresholds(attributeName, valueNode));
                    } else {
                        throw new SAXException("Invalid Threshold Configuration: child of 'attributeName' element was not 'noMatchRule', or 'threshold'");
                    }
                }
                if (noMatchRule == null) {
                    throw new SAXException("Invalid Threshold Configuration: no 'noMatchRule' element found for 'attributeName' element");
                }

                Map<String, CompareRecord> comparisonRecords = thresholdsMap.get(attributeName);
                if (comparisonRecords == null) {
                    comparisonRecords = new HashMap<>();
                    thresholdsMap.put(attributeName, comparisonRecords);
                }

                comparisonRecords.put(noMatchRule.getAttributeValue(), noMatchRule);
                for (final CompareRecord rs : thresholds) {
                    comparisonRecords.put(rs.getAttributeValue(), rs);
                }
            }
        } // end loop through all listed attributes

        return thresholdsMap;
    }
}
