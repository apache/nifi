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

import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ExternalResourceListAsHtmlParserTest extends AbstractHttpsExternalResourceProviderTest {

    @Test
    void parseEmptyResponse() throws XPathExpressionException, ParserConfigurationException, IOException, SAXException {
        final String emptyHtml = HtmlBuilder.newInstance()
                .htmlStart()
                .htmlEnd()
                .build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();
        final Collection<ExternalResourceDescriptor> expected = Collections.emptySet();

        final Collection<ExternalResourceDescriptor> result = parser.parseResponse(emptyHtml, "https://test/");

        assertSuccess(expected, result);
    }

    @Test
    void parseResponseWithParentItemOnly() throws XPathExpressionException, ParserConfigurationException, IOException, SAXException {
        final String parentOnlyHtml = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .tableEnd()
                .htmlEnd()
                .build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();
        final Collection<ExternalResourceDescriptor> expected = Collections.emptySet();

        final Collection<ExternalResourceDescriptor> result = parser.parseResponse(parentOnlyHtml, "https://test/");

        assertSuccess(expected, result);
    }

    @Test
    void parseResponseWithFileItemsOnly() throws XPathExpressionException, ParserConfigurationException, IOException, SAXException {
        final String fileOnlyHtml = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .fileEntry("file1", "2021-05-17 21:09", "size")
                .tableEnd()
                .htmlEnd()
                .build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();

        final Collection<ExternalResourceDescriptor> expected = new HashSet<>();
        final ExternalResourceDescriptor file1 = new ImmutableExternalResourceDescriptor("file1", 1621285740000L, "https://test/", false);
        expected.add(file1);

        final Collection<ExternalResourceDescriptor> result = parser.parseResponse(fileOnlyHtml, "https://test/");

        assertSuccess(expected, result);
    }

    @Test
    void parseResponseWithDirectoryItemsOnly() throws XPathExpressionException, ParserConfigurationException, IOException, SAXException {
        final String directoryOnlyHtml = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .directoryEntry("dir1")
                .tableEnd()
                .htmlEnd()
                .build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();

        final Collection<ExternalResourceDescriptor> expected = new HashSet<>();
        final ExternalResourceDescriptor dir1 = new ImmutableExternalResourceDescriptor("dir1/", 0, "https://test/", true);
        expected.add(dir1);

        final Collection<ExternalResourceDescriptor> result = parser.parseResponse(directoryOnlyHtml, "https://test/");

        assertSuccess(expected, result);
    }

    @Test
    void parseResponseWithMixedItems() throws XPathExpressionException, ParserConfigurationException, IOException, SAXException {
        final String mixedHtml = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .directoryEntry("dir1")
                .fileEntry("file1", "2021-05-17 21:09", "size")
                .tableEnd()
                .htmlEnd()
                .build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();

        final Collection<ExternalResourceDescriptor> expected = new HashSet<>();
        final ExternalResourceDescriptor dir1 = new ImmutableExternalResourceDescriptor("dir1/", 0, "https://test/", true);
        final ExternalResourceDescriptor file1 = new ImmutableExternalResourceDescriptor("file1", 1621285740000L, "https://test/", false);
        expected.add(dir1);
        expected.add(file1);

        final Collection<ExternalResourceDescriptor> result = parser.parseResponse(mixedHtml, "https://test/");

        assertSuccess(expected, result);
    }

    @Test
    void parseResponseWithIncorrectHtml() throws XPathExpressionException {
        final String incorrectHtml = HtmlBuilder.newInstance().htmlStart().build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();

        assertThrows(SAXException.class, () -> parser.parseResponse(incorrectHtml, "https://test/"));
    }

    @Test
    void parseResponseWithIncorrectDate() throws XPathExpressionException {
        final String htmlWithIncorrectDate = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .fileEntry("file1", "-", "size")
                .tableEnd()
                .htmlEnd()
                .build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();

        assertThrows(DateTimeParseException.class, () -> parser.parseResponse(htmlWithIncorrectDate, "https://test/"));
    }

    @Test
    void parseResponseWithIncorrectDateFormat() throws XPathExpressionException {
        final String htmlWithIncorrectDateFormat = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .fileEntry("file1", "2021.05.17 21:09", "size")
                .tableEnd()
                .htmlEnd()
                .build();

        final ExternalResourceListAsHtmlParser parser = new ExternalResourceListAsHtmlParser();

        assertThrows(DateTimeParseException.class, () -> parser.parseResponse(htmlWithIncorrectDateFormat, "https://test/"));
    }
}