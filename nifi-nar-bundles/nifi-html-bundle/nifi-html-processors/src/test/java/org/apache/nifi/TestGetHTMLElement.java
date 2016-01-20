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
package org.apache.nifi;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Selector;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.Exception;
import java.net.URL;
import java.util.List;

public class TestGetHTMLElement extends AbstractHTMLTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GetHTMLElement.class);
        testRunner.setProperty(GetHTMLElement.URL, "http://localhost");
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_HTML);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.HTML_CHARSET, "UTF-8");
    }

    @Test
    public void testCSSSelectorSyntaxValidator() throws IOException {
        Document doc = Jsoup.parse(new URL("http://www.google.com"), 5000);
        try {
            doc.select("---jeremy");
        } catch (Selector.SelectorParseException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testNoElementFound() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "b");   //Bold element is not present in sample HTML

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 1);
    }

    @Test
    public void testInvalidSelector() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "InvalidCSSSelectorSyntax");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 1);
    }

    @Test
    public void testSingleElementFound() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "head");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);
    }

    @Test
    public void testMultipleElementFound() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "a");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 3);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);
    }

    @Test
    public void testElementFoundWriteToAttribute() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "href");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        ffs.get(0).assertAttributeEquals(GetHTMLElement.HTML_ELEMENT_ATTRIBUTE_NAME, ATL_WEATHER_LINK);
    }

    @Test
    public void testElementFoundWriteToContent() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "href");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        ffs.get(0).assertContentEquals(ATL_WEATHER_LINK);
    }

    @Test
    public void testValidPrependValueToFoundElement() throws Exception {
        final String PREPEND_VALUE = "TestPrepend";
        testRunner.setProperty(GetHTMLElement.PREPEND_ELEMENT_VALUE, PREPEND_VALUE);
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "href");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        ffs.get(0).assertContentEquals(PREPEND_VALUE + ATL_WEATHER_LINK);
    }

    @Test
    public void testValidPrependValueToNotFoundElement() throws Exception {
        final String PREPEND_VALUE = "TestPrepend";
        testRunner.setProperty(GetHTMLElement.PREPEND_ELEMENT_VALUE, PREPEND_VALUE);
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "b");
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_TEXT);

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 1);
    }

    @Test
    public void testValidAppendValueToFoundElement() throws Exception {
        final String APPEND_VALUE = "TestAppend";
        testRunner.setProperty(GetHTMLElement.APPEND_ELEMENT_VALUE, APPEND_VALUE);
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "href");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        ffs.get(0).assertContentEquals(ATL_WEATHER_LINK + APPEND_VALUE);
    }

    @Test
    public void testValidAppendValueToNotFoundElement() throws Exception {
        final String APPEND_VALUE = "TestAppend";
        testRunner.setProperty(GetHTMLElement.APPEND_ELEMENT_VALUE, APPEND_VALUE);
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "b");
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_TEXT);

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 1);
    }

    @Test
    public void testExtractAttributeFromElement() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "meta[name=author]");
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "Content");

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        ffs.get(0).assertContentEquals(AUTHOR_NAME);
    }

    @Test
    public void testExtractTextFromElement() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_TEXT);

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        ffs.get(0).assertContentEquals(ATL_WEATHER_TEXT);
    }

    @Test
    public void testExtractHTMLFromElement() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + GDR_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_HTML);

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        ffs.get(0).assertContentEquals(GDR_WEATHER_TEXT);
    }
}
