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
package org.apache.nifi.flow.resource;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;

public class ExternalResourceListAsHtmlParser {
    private static final String DEFAULT_X_PATH_IDENTIFIER_FOR_FILE_LIST = "//tr[count(td)>2]";
    private static final String DEFAULT_X_PATH_IDENTIFIER_FOR_LOCATION = "./td[1]/a/text()";
    private static final String DEFAULT_X_PATH_IDENTIFIER_FOR_LAST_MODIFIED = "./td[2]/text()";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyy-MM-dd HH:mm");

    private final XPathExpression xPathIdentifierForFileList;
    private final XPathExpression xPathIdentifierForLocation;
    private final XPathExpression xPathIdentifierForLastModified;

    public ExternalResourceListAsHtmlParser() throws XPathExpressionException {
        final XPath xPath = XPathFactory.newInstance().newXPath();
        xPathIdentifierForFileList = xPath.compile(DEFAULT_X_PATH_IDENTIFIER_FOR_FILE_LIST);
        xPathIdentifierForLocation = xPath.compile(DEFAULT_X_PATH_IDENTIFIER_FOR_LOCATION);
        xPathIdentifierForLastModified = xPath.compile(DEFAULT_X_PATH_IDENTIFIER_FOR_LAST_MODIFIED);
    }

    public Collection<ExternalResourceDescriptor> parseResponse(final String response, final String url) throws ParserConfigurationException, XPathExpressionException, IOException, SAXException {
        final Collection<ExternalResourceDescriptor> result = new ArrayList<>();
        final NodeList nodeList = gatherResourceNodeList(response);
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (isFile(node)) {
                final ExternalResourceDescriptor descriptor = createDescriptor(node, url, true);
                result.add(descriptor);
            } else {
                final ExternalResourceDescriptor descriptor = createDescriptor(node, url, false);
                result.add(descriptor);
            }
        }
        return result;
    }

    private NodeList gatherResourceNodeList(final String response) throws ParserConfigurationException, XPathExpressionException, IOException, SAXException {
        final Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(new StringReader(response)));
        return (NodeList) xPathIdentifierForFileList.evaluate(doc, XPathConstants.NODESET);
    }

    private boolean isFile(Node node) throws XPathExpressionException {
        return !xPathIdentifierForLocation.evaluate(node).endsWith("/");
    }

    private ImmutableExternalResourceDescriptor createDescriptor(final Node node, final String url, final boolean isFile) throws XPathExpressionException {
        if (isFile) {
            return new ImmutableExternalResourceDescriptor(parseLocation(node),
                    parseLastModified(node),
                    url,
                    false);
        }
        return new ImmutableExternalResourceDescriptor(parseLocation(node),
                0,
                url,
                true);
    }

    private String parseLocation(final Node node) throws XPathExpressionException {
            return xPathIdentifierForLocation.evaluate(node);
    }

    private long parseLastModified(final Node node) throws XPathExpressionException {
        final String time = xPathIdentifierForLastModified.evaluate(node);
        return LocalDateTime.parse(time, FORMATTER).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }
}
