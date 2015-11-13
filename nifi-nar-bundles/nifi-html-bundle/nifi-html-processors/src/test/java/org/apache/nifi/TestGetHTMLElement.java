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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.lang.Exception;
import java.util.List;

import static org.junit.Assert.assertTrue;

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
    public void testNoElementFound() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "b");   //Bold element is not present in sample HTML
//        testRunner.setProperty(GetHTMLElement.APPEND_ELEMENT_VALUE, "");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 1);
    }

    @Test
    public void testInvalidSelector() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "InvalidCSSSelectorSyntax");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 1);
    }

    @Test
    public void testSingleElementFound() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "head");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);
    }

    @Test
    public void testMultipleElementFound() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "a");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 3);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);
    }

    @Test
    public void testElementFoundWriteToAttribute() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "href");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        MockFlowFile fff = ffs.get(0);
        String atValue = fff.getAttribute(GetHTMLElement.HTML_ELEMENT_ATTRIBUTE_NAME);
        assertTrue(StringUtils.equals(ATL_WEATHER_LINK, atValue));
    }

    @Test
    public void testElementFoundWriteToContent() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "href");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));
        assertTrue(StringUtils.equals(ATL_WEATHER_LINK, data));
    }

    @Test
    public void testValidPrependValueToFoundElement() throws Exception {
        final String PREPEND_VALUE = "TestPrepend";
        testRunner.setProperty(GetHTMLElement.PREPEND_ELEMENT_VALUE, PREPEND_VALUE);
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "href");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));
        assertTrue(StringUtils.equals(PREPEND_VALUE + ATL_WEATHER_LINK, data));
    }

    @Test
    public void testValidPrependValueToNotFoundElement() throws Exception {
        final String PREPEND_VALUE = "TestPrepend";
        testRunner.setProperty(GetHTMLElement.PREPEND_ELEMENT_VALUE, PREPEND_VALUE);
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "b");
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_TEXT);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
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

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));
        assertTrue(StringUtils.equals(ATL_WEATHER_LINK + APPEND_VALUE, data));
    }

    @Test
    public void testValidAppendValueToNotFoundElement() throws Exception {
        final String APPEND_VALUE = "TestAppend";
        testRunner.setProperty(GetHTMLElement.APPEND_ELEMENT_VALUE, APPEND_VALUE);
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "b");
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_TEXT);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 1);
    }

    @Test
    public void testExtractAttributeFromElement() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "meta[name=author]");
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_ATTRIBUTE);
        testRunner.setProperty(GetHTMLElement.ATTRIBUTE_KEY, "Content");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));
        assertTrue(StringUtils.equals(AUTHOR_NAME, data));
    }

    @Test
    public void testExtractTextFromElement() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + ATL_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_TEXT);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));
        assertTrue(StringUtils.equals(ATL_WEATHER_TEXT, data));
    }

    @Test
    public void testExtractHTMLFromElement() throws Exception {
        testRunner.setProperty(GetHTMLElement.CSS_SELECTOR, "#" + GDR_ID);
        testRunner.setProperty(GetHTMLElement.DESTINATION, GetHTMLElement.DESTINATION_CONTENT);
        testRunner.setProperty(GetHTMLElement.OUTPUT_TYPE, GetHTMLElement.ELEMENT_HTML);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = writeContentToNewFlowFile(HTML.getBytes(), session);

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(GetHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_FAILURE, 0);
        testRunner.assertTransferCount(GetHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(GetHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(GetHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));
        assertTrue(StringUtils.equals(GDR_WEATHER_TEXT, data));
    }
}
