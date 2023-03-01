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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.kafkamigrator.MigratorConfiguration;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractKafkaMigrator implements Migrator {
    static final XPath XPATH = XPathFactory.newInstance().newXPath();
    private final static String NEW_KAFKA_PROCESSOR_VERSION = "_2_0";
    private final static String ARTIFACT = "nifi-kafka-2-0-nar";
    private final static String PATH_FOR_ARTIFACT = "bundle/artifact";

    final boolean isVersion8Processor;
    final boolean isKafkaBrokersPresent;

    final Map<String, String> kafkaProcessorProperties;
    final Map<String, String> propertiesToBeSaved;
    final Map<String, String> controllerServices;

    final String xpathForProperties;
    final String propertyKeyTagName;
    final String propertyTagName;

    final String xpathForTransactionProperty;
    final String transactionTagName;
    final boolean transaction;


    public AbstractKafkaMigrator(final MigratorConfiguration configuration) {
        final String kafkaBrokers = configuration.getKafkaBrokers();
        this.isKafkaBrokersPresent = !kafkaBrokers.isEmpty();
        this.isVersion8Processor = configuration.isVersion8Processor();
        this.kafkaProcessorProperties = new HashMap<>(configuration.getProcessorDescriptor().getProcessorProperties());
        this.propertiesToBeSaved = configuration.getProcessorDescriptor().getPropertiesToBeSaved();
        this.controllerServices = configuration.getProcessorDescriptor().getControllerServicesForTemplates();
        this.xpathForProperties = configuration.getPropertyXpathDescriptor().getXpathForProperties();
        this.propertyKeyTagName = configuration.getPropertyXpathDescriptor().getPropertyKeyTagName();
        this.propertyTagName = configuration.getPropertyXpathDescriptor().getPropertyTagName();
        this.xpathForTransactionProperty = configuration.getPropertyXpathDescriptor().getXpathForTransactionProperty();
        this.transactionTagName = configuration.getPropertyXpathDescriptor().getTransactionTagName();
        this.transaction = configuration.isTransaction();

        if (isKafkaBrokersPresent) {
            kafkaProcessorProperties.put("bootstrap.servers", kafkaBrokers);
        }
    }

    @Override
    public void configureProperties(final Node node) throws XPathExpressionException {
        if (isVersion8Processor && isKafkaBrokersPresent) {
            final NodeList properties = (NodeList) XPATH.evaluate(xpathForProperties, node, XPathConstants.NODESET);
            for (int i = 0; i < properties.getLength(); i++) {
                final Node property = properties.item(i);
                saveRequiredProperties(property);
                removeElement(node, property);
            }
            addNewProperties(node);
        }
    }

    @Override
    public void configureDescriptors(final Node node) throws XPathExpressionException {
        if(isVersion8Processor && isKafkaBrokersPresent) {
            final Element descriptorElement = (Element) XPATH.evaluate("config/descriptors", node, XPathConstants.NODE);
            final NodeList descriptors = (NodeList) XPATH.evaluate("entry", descriptorElement, XPathConstants.NODESET);
            for (int i = 0; i < descriptors.getLength(); i++) {
                final Node descriptor = descriptors.item(i);
                removeElement(descriptorElement, descriptor);
            }
            addNewDescriptors(descriptorElement);
        }
    }

    @Override
    public void configureComponentSpecificSteps(final Node node) throws XPathExpressionException {
        final String transactionString = Boolean.toString(transaction);
        final Element transactionsElement = (Element) XPATH.evaluate(xpathForTransactionProperty, node, XPathConstants.NODE);

        if (transactionsElement != null) {
            transactionsElement.setTextContent(transactionString);
        } else {
            addNewProperty(node, transactionTagName, transactionString);
        }

        kafkaProcessorProperties.put(transactionTagName, transactionString);
    }

    public void replaceClassName(final Element className) {
        final String processorName = StringUtils.substringAfterLast(className.getTextContent(), ".");
        final String newClassName = replaceClassNameWithNewProcessorName(className.getTextContent(), processorName);
        className.setTextContent(newClassName);
    }

    public void replaceArtifact(final Node processor) throws XPathExpressionException {
        ((Element) XPATH.evaluate(PATH_FOR_ARTIFACT, processor, XPathConstants.NODE)).setTextContent(ARTIFACT);
    }

    private static String replaceClassNameWithNewProcessorName(final String className, final String processorName) {
        final String newProcessorName = StringUtils.replaceEach(processorName, new String[]{"Get", "Put"}, new String[]{"pubsub.Consume", "pubsub.Publish"});
        final String processorNameWithNewVersion =
                newProcessorName.replaceFirst("$|_0_1\\d?", NEW_KAFKA_PROCESSOR_VERSION);
        return StringUtils.replace(className, processorName, processorNameWithNewVersion);
    }

    private void addNewDescriptors(final Node node) {
        for (String key: kafkaProcessorProperties.keySet()) {
            final Element descriptorElement = node.getOwnerDocument().createElement("entry");
            node.appendChild(descriptorElement);

            final Element descriptorKeyElement = descriptorElement.getOwnerDocument().createElement("key");
            descriptorKeyElement.setTextContent(key);
            descriptorElement.appendChild(descriptorKeyElement);

            final Element descriptorValueElement = descriptorElement.getOwnerDocument().createElement("value");
            descriptorElement.appendChild(descriptorValueElement);

            final Element descriptorNameElement = descriptorValueElement.getOwnerDocument().createElement("name");
            descriptorNameElement.setTextContent(key);
            descriptorValueElement.appendChild(descriptorNameElement);

            if (controllerServices.containsKey(key)) {
                final Element controllerServiceElement = descriptorValueElement.getOwnerDocument().createElement("identifiesControllerService");
                controllerServiceElement.setTextContent(controllerServices.get(key));
                descriptorValueElement.appendChild(controllerServiceElement);
            }
        }
    }

    private void saveRequiredProperties(final Node property) throws XPathExpressionException {
        final String propertyToBeSaved = propertiesToBeSaved.get(XPATH.evaluate(propertyKeyTagName, property));

        if (propertyToBeSaved != null) {
            String propertyValue = XPATH.evaluate("value", property);
            kafkaProcessorProperties.put(propertyToBeSaved, convert(propertyValue));
        }
    }

    private String convert(final String propertyValue) {
        return propertyValue.isEmpty() ? null : propertyValue;
    }

    private void addNewProperties(final Node node) {
        for (Map.Entry<String, String> entry : kafkaProcessorProperties.entrySet()) {
            addNewProperty(node, entry.getKey(), entry.getValue());
        }
    }

    private void addNewProperty(final Node node, final String key, final String value) {
        final Element propertyElement = node.getOwnerDocument().createElement(propertyTagName);
        node.appendChild(propertyElement);

        final Element propertyKeyElement = propertyElement.getOwnerDocument().createElement(propertyKeyTagName);
        propertyKeyElement.setTextContent(key);

        propertyElement.appendChild(propertyKeyElement);

        if (value != null) {
            final Element propertyValueElement = propertyElement.getOwnerDocument().createElement("value");
            propertyValueElement.setTextContent(value);

            propertyElement.appendChild(propertyValueElement);
        }
    }

    private void removeElement(final Node node, final Node element) {
        node.removeChild(element);
    }
}
