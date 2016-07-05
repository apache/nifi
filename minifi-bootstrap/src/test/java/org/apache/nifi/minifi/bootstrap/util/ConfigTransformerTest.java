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

package org.apache.nifi.minifi.bootstrap.util;

import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConfigTransformerTest {

    private XPathFactory xPathFactory;
    private Element config;

    @Before
    public void setup() throws ParserConfigurationException {
        config = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument().createElement("config");
        xPathFactory = XPathFactory.newInstance();
    }

    @Test
    public void testNullQueuePrioritizerNotWritten() throws ConfigurationChangeException, XPathExpressionException {
        ConfigTransformer.addConnection(config, new ConnectionSchema(Collections.emptyMap()), new ConfigSchema(Collections.emptyMap()));
        XPath xpath = xPathFactory.newXPath();
        String expression = "connection/queuePrioritizerClass";
        assertNull(xpath.evaluate(expression, config, XPathConstants.NODE));
    }

    @Test
    public void testEmptyQueuePrioritizerNotWritten() throws ConfigurationChangeException, XPathExpressionException {
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, "");

        ConfigTransformer.addConnection(config, new ConnectionSchema(map), new ConfigSchema(Collections.emptyMap()));
        XPath xpath = xPathFactory.newXPath();
        String expression = "connection/queuePrioritizerClass";
        assertNull(xpath.evaluate(expression, config, XPathConstants.NODE));
    }

    @Test
    public void testQueuePrioritizerWritten() throws ConfigurationChangeException, XPathExpressionException {
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, FirstInFirstOutPrioritizer.class.getCanonicalName());

        ConfigTransformer.addConnection(config, new ConnectionSchema(map), new ConfigSchema(Collections.emptyMap()));
        XPath xpath = xPathFactory.newXPath();
        String expression = "connection/queuePrioritizerClass/text()";
        assertEquals(FirstInFirstOutPrioritizer.class.getCanonicalName(), xpath.evaluate(expression, config, XPathConstants.STRING));
    }
}
