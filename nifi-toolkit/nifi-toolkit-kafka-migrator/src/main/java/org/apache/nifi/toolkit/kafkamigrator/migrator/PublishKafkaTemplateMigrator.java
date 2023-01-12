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
package org.apache.nifi.toolkit.kafkamigrator.migrator;

import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorDescriptor;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.util.Map;

public class PublishKafkaTemplateMigrator extends AbstractKafkaMigrator {
    private static final String XPATH_FOR_TRANSACTION_PROPERTY = "entry[key=\"use-transactions\"]/value";
    private static final String TRANSACTION_TAG_NAME = "use-transactions";

    public PublishKafkaTemplateMigrator(final Map<String, String> arguments, final boolean isVersion8Processor) {
        super(arguments, isVersion8Processor,
                new KafkaProcessorDescriptor("Publish"),
                "entry",
                "key", "entry",
                XPATH_FOR_TRANSACTION_PROPERTY, TRANSACTION_TAG_NAME);
    }

    @Override
    public void configureProperties(final Node node) throws XPathExpressionException {
        final Element propertyElement = (Element) XPATH.evaluate("config/properties", node, XPathConstants.NODE);
        super.configureProperties(propertyElement);
    }

    @Override
    public void configureDescriptors(final Node node) throws XPathExpressionException {
        super.configureDescriptors(node);
    }

    @Override
    public void configureComponentSpecificSteps(final Node node, final Map<String, String> properties) throws XPathExpressionException {
        //add value if null
        final Element propertyElement = (Element) XPATH.evaluate("config/properties", node, XPathConstants.NODE);
        final Element deliveryGuaranteeValue = (Element) XPATH.evaluate("entry[key=\"acks\"]/value", propertyElement, XPathConstants.NODE);
        if (Boolean.parseBoolean(properties.get("transaction")) && deliveryGuaranteeValue != null) {
            deliveryGuaranteeValue.setTextContent("all");
        }
        super.configureComponentSpecificSteps(propertyElement, properties);
    }
}
