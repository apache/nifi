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
package org.apache.nifi.toolkit.kafkamigrator;

import org.apache.nifi.toolkit.kafkamigrator.descriptor.FlowPropertyXpathDescriptor;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorDescriptor;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorType;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.PropertyXpathDescriptor;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.TemplatePropertyXpathDescriptor;
import org.apache.nifi.toolkit.kafkamigrator.migrator.ConsumeKafkaFlowMigrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.ConsumeKafkaTemplateMigrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.PublishKafkaFlowMigrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.PublishKafkaTemplateMigrator;
import org.apache.nifi.toolkit.kafkamigrator.MigratorConfiguration.MigratorConfigurationBuilder;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaMigratorTest {
    private static final XPath XPATH = XPathFactory.newInstance().newXPath();
    private static final String XPATH_FOR_PUBLISH_PROCESSOR_IN_FLOW = ".//processor[class='org.apache.nifi.processors.kafka.PutKafka']";
    private static final String XPATH_FOR_PUBLISH_PROCESSOR_IN_TEMPLATE = ".//processors[type='org.apache.nifi.processors.kafka.PutKafka']";

    private static final String XPATH_FOR_CONSUME_PROCESSOR_IN_FLOW = ".//processor[class='org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_0_10']";
    private static final String XPATH_FOR_CONSUME_PROCESSOR_IN_TEMPLATE = ".//processors[type='org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_0_10']";
    private static final KafkaProcessorDescriptor PUBLISH_KAFKA_PROCESSOR_DESCRIPTOR = new KafkaProcessorDescriptor(KafkaProcessorType.PUBLISH);
    private static final boolean WITH_TRANSACTION = Boolean.TRUE;
    private static final boolean WITHOUT_TRANSACTION = Boolean.FALSE;
    private static final boolean IS_VERSION_EIGHT_PROCESSOR = Boolean.TRUE;
    private static final boolean IS_NOT_VERSION_EIGHT_PROCESSOR = Boolean.FALSE;
    private static final String FLOW = "Flow";
    private static final String TEMPLATE = "Template";

    @Test
    public void testPropertiesRemoved() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, FLOW);
        final PublishKafkaFlowMigrator flowMigrator = new PublishKafkaFlowMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_FLOW, document, XPathConstants.NODE);

        flowMigrator.configureProperties(processor);

        assertPropertyRemoveSuccess(processor);
    }

    @Test
    public void testPropertiesAdded() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR,KafkaProcessorType.PUBLISH, FLOW);
        final PublishKafkaFlowMigrator flowMigrator = new PublishKafkaFlowMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_FLOW, document, XPathConstants.NODE);

        flowMigrator.configureProperties(processor);

        assertPropertyAddSuccess(processor);
    }

    @Test
    public void testPropertiesSaved() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, FLOW);
        final PublishKafkaFlowMigrator flowMigrator = new PublishKafkaFlowMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_FLOW, document, XPathConstants.NODE);
        final List<String> oldValues = getOldValues(processor);

        flowMigrator.configureProperties(processor);

        final List<String> newValues = getNewValues(processor);
        assertEquals(oldValues, newValues);
    }

    @Test
    public void testDescriptorsAdded() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, TEMPLATE);
        final PublishKafkaTemplateMigrator templateMigrator = new PublishKafkaTemplateMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_TEMPLATE, document, XPathConstants.NODE);

        templateMigrator.configureDescriptors(processor);

        assertDescriptorAddSuccess(processor);
    }

    @Test
    public void testDescriptorsRemoved() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, TEMPLATE);
        final PublishKafkaTemplateMigrator templateMigrator = new PublishKafkaTemplateMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_TEMPLATE, document, XPathConstants.NODE);

        templateMigrator.configureDescriptors(processor);

        assertDescriptorRemoveSuccess(processor);
    }

    @Test
    public void testTransactionFlowPropertyForConsumeProcessorWithTrue() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITH_TRANSACTION, IS_NOT_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.CONSUME, FLOW);
        final ConsumeKafkaFlowMigrator flowMigrator = new ConsumeKafkaFlowMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_CONSUME_PROCESSOR_IN_FLOW, document, XPathConstants.NODE);

        flowMigrator.configureComponentSpecificSteps(processor);

        assertEquals("true", XPATH.evaluate("property[name='honor-transactions']/value", processor));
    }

    @Test
    public void testTransactionTemplatePropertyForConsumeProcessorWithTrue() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITH_TRANSACTION, IS_NOT_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.CONSUME, TEMPLATE);
        final ConsumeKafkaTemplateMigrator templateMigrator = new ConsumeKafkaTemplateMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_CONSUME_PROCESSOR_IN_TEMPLATE, document, XPathConstants.NODE);

        templateMigrator.configureComponentSpecificSteps(processor);

        assertEquals("true", XPATH.evaluate("config/properties/entry[key='honor-transactions']/value", processor));
    }

    @Test
    public void testTransactionFlowPropertyForConsumeProcessorWithFalse() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_NOT_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.CONSUME, FLOW);
        final ConsumeKafkaFlowMigrator flowMigrator = new ConsumeKafkaFlowMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_CONSUME_PROCESSOR_IN_FLOW, document, XPathConstants.NODE);

        flowMigrator.configureComponentSpecificSteps(processor);

        assertEquals("false", XPATH.evaluate("property[name='honor-transactions']/value", processor));
    }

    @Test
    public void testTransactionTemplatePropertyForConsumeProcessorWithFalse() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_NOT_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.CONSUME, TEMPLATE);
        final ConsumeKafkaTemplateMigrator templateMigrator = new ConsumeKafkaTemplateMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_CONSUME_PROCESSOR_IN_TEMPLATE, document, XPathConstants.NODE);

        templateMigrator.configureComponentSpecificSteps(processor);

        assertEquals("false", XPATH.evaluate("config/properties/entry[key='honor-transactions']/value", processor));
    }

    @Test
    public void testTransactionFlowPropertyForPublishProcessorWithTrue() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITH_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, FLOW);
        final PublishKafkaFlowMigrator flowMigrator = new PublishKafkaFlowMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_FLOW, document, XPathConstants.NODE);

        flowMigrator.configureComponentSpecificSteps(processor);

        assertEquals("true", XPATH.evaluate("property[name='use-transactions']/value", processor));
        assertEquals("", XPATH.evaluate("property[name='acks']/value", processor));

    }

    @Test
    public void testTransactionTemplatePropertyForPublishProcessorWithTrue() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITH_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, TEMPLATE);
        final PublishKafkaTemplateMigrator templateMigrator = new PublishKafkaTemplateMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_TEMPLATE, document, XPathConstants.NODE);

        templateMigrator.configureComponentSpecificSteps(processor);

        assertEquals("true", XPATH.evaluate("config/properties/entry[key='use-transactions']/value", processor));
        assertEquals("", XPATH.evaluate("config/properties/entry[key='acks']/value", processor));
    }

    @Test
    public void testTransactionFlowPropertyForPublishProcessorWithFalse() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, FLOW);
        final PublishKafkaFlowMigrator flowMigrator = new PublishKafkaFlowMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_FLOW, document, XPathConstants.NODE);

        flowMigrator.configureComponentSpecificSteps(processor);

        assertEquals("false", XPATH.evaluate("property[name='use-transactions']/value", processor));
    }

    @Test
    public void testTransactionTemplatePropertyForPublishProcessorWithFalse() throws XPathExpressionException, IOException {
        final MigratorConfiguration configuration = getConfiguration(WITHOUT_TRANSACTION, IS_VERSION_EIGHT_PROCESSOR, KafkaProcessorType.PUBLISH, TEMPLATE);
        final PublishKafkaTemplateMigrator templateMigrator = new PublishKafkaTemplateMigrator(configuration);
        final Document document = KafkaMigrationUtil.parseDocument();
        final Node processor = (Node) XPATH.evaluate(XPATH_FOR_PUBLISH_PROCESSOR_IN_TEMPLATE, document, XPathConstants.NODE);

        templateMigrator.configureComponentSpecificSteps(processor);

        assertEquals("false", XPATH.evaluate("config/properties/entry[key='use-transactions']/value", processor));
    }


    private List<String> getValues(final Collection<String> properties, final Node node) throws XPathExpressionException {
        final List<String> result = new ArrayList<>();
        for (String propertyName : properties) {
            result.add(XPATH.evaluate(String.format("property[name='%s']/value", propertyName), node));
        }
        return result;
    }

    private List<String> getOldValues(final Node node) throws XPathExpressionException {
        return getValues(PUBLISH_KAFKA_PROCESSOR_DESCRIPTOR.getPropertiesToBeSaved().keySet(), node);
    }

    private List<String> getNewValues(final Node node) throws XPathExpressionException {
        return getValues(PUBLISH_KAFKA_PROCESSOR_DESCRIPTOR.getPropertiesToBeSaved().values(), node);
    }

    private void assertPropertyRemoveSuccess(final Node node) throws XPathExpressionException {
        assertTrue(XPATH.evaluate("property[name='Known Brokers']", node).isEmpty());
    }

    private void assertDescriptorRemoveSuccess(final Node node) throws XPathExpressionException {
        assertTrue(XPATH.evaluate("config/descriptors/entry[key='Known Brokers']", node).isEmpty());
    }

    private void assertAddSuccess(final String xpath, final Node node) throws XPathExpressionException {
        for (String propertyName: PUBLISH_KAFKA_PROCESSOR_DESCRIPTOR.getProcessorProperties().keySet()) {
            assertFalse(XPATH.evaluate(String.format(xpath, propertyName), node).isEmpty());
        }
    }
    private void assertPropertyAddSuccess(final Node node) throws XPathExpressionException {
        assertAddSuccess("property[name='%s']/name", node);
    }

    private void assertDescriptorAddSuccess(final Node node) throws XPathExpressionException {
        assertAddSuccess("config/descriptors/entry[key='%s']/key", node);
    }

    private MigratorConfiguration getConfiguration(final boolean transaction, final boolean isVersion8Processor,
                                                   final KafkaProcessorType processorType, final String migrationType) {
        final MigratorConfigurationBuilder configurationBuilder = new MigratorConfigurationBuilder();
        final PropertyXpathDescriptor propertyXpathDescriptor;

        if (migrationType.equalsIgnoreCase("Flow")) {
            propertyXpathDescriptor = new FlowPropertyXpathDescriptor(processorType);
        } else {
             propertyXpathDescriptor= new TemplatePropertyXpathDescriptor(processorType);
        }

        return configurationBuilder.setKafkaBrokers("kafkaBrokers, localhost:1234")
                                    .setTransaction(transaction)
                                    .setIsVersion8Processor(isVersion8Processor)
                                    .setProcessorDescriptor(new KafkaProcessorDescriptor(processorType))
                                    .setPropertyXpathDescriptor(propertyXpathDescriptor)
                                    .build();
    }
}