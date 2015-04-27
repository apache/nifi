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
package org.apache.nifi.processors.standard.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XmlSplitterSaxParser extends DefaultHandler {

    private final XmlElementNotifier notifier;
    private final AtomicInteger splitDepth;
    private final StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    public Locator locator;
    private final AtomicInteger depth = new AtomicInteger(0);

    public XmlSplitterSaxParser(final XmlElementNotifier notifier) {
        this.notifier = notifier;
        this.splitDepth = new AtomicInteger(1);
    }

    public XmlSplitterSaxParser(final XmlElementNotifier notifier, int splitDepth) {
        this.notifier = notifier;
        this.splitDepth = new AtomicInteger(splitDepth);
    }

    @Override
    public void characters(final char[] ch, final int start, final int length) throws SAXException {
        for (int i = start; i < start + length; i++) {
            char c = ch[i];
            switch (c) {
                case '<':
                    sb.append("&lt;");
                    break;
                case '>':
                    sb.append("&gt;");
                    break;
                case '&':
                    sb.append("&amp;");
                    break;
                case '\'':
                    sb.append("&apos;");
                    break;
                case '"':
                    sb.append("&quot;");
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
    }

    @Override
    public void endElement(final String uri, final String localName, final String qName) throws SAXException {
        // Add the element end tag.
        sb.append("</").
                append(qName).
                append(">");

        // We have finished processing this element. Decrement the depth.
        int newDepth = depth.decrementAndGet();
        // If we have now returned to level 1, we have finished processing
        // a 2nd-level element. Send notification with the XML text and
        // erase the String Builder so that we can start
        // processing a new 2nd-level element.
        if (newDepth == splitDepth.get()) {

            String elementTree = sb.toString();
            notifier.onXmlElementFound(elementTree);
            // Reset the StringBuilder to just the XML prolog.
            sb.setLength(38);
        }
    }

    @Override
    public void startElement(final String uri, final String localName, final String qName, final Attributes atts) throws SAXException {
        // Increment the current depth because start a new XML element.
        int newDepth = depth.incrementAndGet();
        // Output the element and its attributes if it is
        // not the root element.
        if (newDepth > splitDepth.get()) {
            sb.append("<");
            sb.append(qName);

            int attCount = atts.getLength();
            for (int i = 0; i < attCount; i++) {
                String attName = atts.getQName(i);
                String attValue = atts.getValue(i);
                sb.append(" ").
                        append(attName).
                        append("=").
                        append("\"").
                        append(attValue).
                        append("\"");
            }

            sb.append(">");
        }
    }

}
