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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import static org.junit.Assert.assertTrue;


public class TestPutHTMLElement extends AbstractHTMLTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PutHTMLElement.class);
        testRunner.setProperty(PutHTMLElement.URL, "http://localhost");
    }

    @Test
    public void testAddNewElementToRoot() throws Exception {
        final String MOD_VALUE = "<p>modified value</p>";
        testRunner.setProperty(PutHTMLElement.CSS_SELECTOR, "body");
        testRunner.setProperty(PutHTMLElement.PUT_LOCATION_TYPE, PutHTMLElement.PREPEND_ELEMENT);
        testRunner.setProperty(PutHTMLElement.PUT_VALUE, MOD_VALUE);

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(PutHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(PutHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(PutHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(PutHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));

        //Contents will be the entire HTML doc. So lets use Jsoup again just the grab the element we want.
        Document doc = Jsoup.parse(data);
        Elements eles = doc.select("body > p");
        Element ele = eles.get(0);

        assertTrue(StringUtils.equals(MOD_VALUE.replace("<p>", "").replace("</p>", ""), ele.html()));
    }

    @Test
    public void testPrependPElementToDiv() throws Exception {
        final String MOD_VALUE = "<p>modified value</p>";
        testRunner.setProperty(PutHTMLElement.CSS_SELECTOR, "#put");
        testRunner.setProperty(PutHTMLElement.PUT_LOCATION_TYPE, PutHTMLElement.PREPEND_ELEMENT);
        testRunner.setProperty(PutHTMLElement.PUT_VALUE, MOD_VALUE);

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(PutHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(PutHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(PutHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(PutHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));

        //Contents will be the entire HTML doc. So lets use Jsoup again just the grab the element we want.
        Document doc = Jsoup.parse(data);
        Elements eles = doc.select("#put");
        Element ele = eles.get(0);

        assertTrue(StringUtils.equals("<p>modified value</p> \n<a href=\"httpd://localhost\"></a>", ele.html()));
    }

    @Test
    public void testAppendPElementToDiv() throws Exception {
        final String MOD_VALUE = "<p>modified value</p>";
        testRunner.setProperty(PutHTMLElement.CSS_SELECTOR, "#put");
        testRunner.setProperty(PutHTMLElement.PUT_LOCATION_TYPE, PutHTMLElement.APPEND_ELEMENT);
        testRunner.setProperty(PutHTMLElement.PUT_VALUE, MOD_VALUE);

        testRunner.enqueue(new File("src/test/resources/Weather.html").toPath());
        testRunner.run();

        testRunner.assertTransferCount(PutHTMLElement.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutHTMLElement.REL_INVALID_HTML, 0);
        testRunner.assertTransferCount(PutHTMLElement.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(PutHTMLElement.REL_NOT_FOUND, 0);

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(PutHTMLElement.REL_SUCCESS);
        assertTrue(ffs.size() == 1);
        String data = new String(testRunner.getContentAsByteArray(ffs.get(0)));

        //Contents will be the entire HTML doc. So lets use Jsoup again just the grab the element we want.
        Document doc = Jsoup.parse(data);
        Elements eles = doc.select("#put");
        Element ele = eles.get(0);

        assertTrue(StringUtils.equals("<a href=\"httpd://localhost\"></a> \n" +
                "<p>modified value</p>", ele.html()));
    }

}
