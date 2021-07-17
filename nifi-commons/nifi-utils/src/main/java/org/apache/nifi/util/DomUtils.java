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
package org.apache.nifi.util;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class DomUtils {

    public static String getChildText(final Element element, final String tagName) {
        final Element childElement = getChild(element, tagName);
        if (childElement == null) {
            return null;
        }

        final String text = childElement.getTextContent();
        return (text == null) ? null : text.trim();
    }

    public static Element getChild(final Element element, final String tagName) {
        final List<Element> children = getChildElementsByTagName(element, tagName);
        if (children.isEmpty()) {
            return null;
        }

        if (children.size() > 1) {
            return null;
        }

        return children.get(0);
    }

    public static List<Element> getChildElementsByTagName(final Element element, final String tagName) {
        final List<Element> matches = new ArrayList<>();
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (!(node instanceof Element)) {
                continue;
            }

            final Element child = (Element) nodeList.item(i);
            if (child.getNodeName().equals(tagName)) {
                matches.add(child);
            }
        }

        return matches;
    }

    public static NodeList getChildNodesByTagName(final Element element, final String tagName) {
        final List<Element> elements = getChildElementsByTagName(element, tagName);

        return new NodeList() {
            @Override
            public Node item(final int index) {
                if (index >= elements.size()) {
                    return null;
                }

                return elements.get(index);
            }

            @Override
            public int getLength() {
                return elements.size();
            }
        };
    }

}
