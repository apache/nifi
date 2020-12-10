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
package org.apache.nifi.minifi;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.File;
import java.util.List;

public class FlowParserTest {

    private static final String TEST_FLOW_XML_PATH = "./src/test/resources/flow.xml.gz";

    @Test
    public void testCanParseFlow() {
        FlowParser flowParser = new FlowParser();
        Document parsedFlow = flowParser.parse(new File(TEST_FLOW_XML_PATH));

        Assert.assertNotNull(parsedFlow);

        NodeList processors = parsedFlow.getElementsByTagName(FlowEnricher.PROCESSOR_TAG_NAME);
        NodeList controllerServices = parsedFlow.getElementsByTagName(FlowEnricher.CONTROLLER_SERVICE_TAG_NAME);
        NodeList reportingTasks = parsedFlow.getElementsByTagName(FlowEnricher.REPORTING_TASK_TAG_NAME);

        Assert.assertEquals("Verify correct number of processors", 3, processors.getLength());
        Assert.assertEquals("Verify correct number of controller services", 1, controllerServices.getLength());
        Assert.assertEquals("Verify correct number of reporting tasks", 0, reportingTasks.getLength());
    }

    @Test
    public void testGetChildrenByTagName() {
        FlowParser flowParser = new FlowParser();
        Document parsedFlow = flowParser.parse(new File(TEST_FLOW_XML_PATH));
        NodeList processors = parsedFlow.getElementsByTagName(FlowEnricher.PROCESSOR_TAG_NAME);

        for (int i = 0; i < processors.getLength(); i++) {
            Element processor = (Element) processors.item(i);

            List<Element> classElements = FlowParser.getChildrenByTagName(processor, "class");
            Assert.assertEquals(1, classElements.size());

            List<Element> propertyElements = FlowParser.getChildrenByTagName(processor, "property");
            Assert.assertTrue(propertyElements.size() > 1);
        }
    }
}
