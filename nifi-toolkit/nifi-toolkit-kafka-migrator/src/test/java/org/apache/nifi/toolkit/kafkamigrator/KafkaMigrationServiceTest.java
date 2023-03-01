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

import org.apache.nifi.toolkit.kafkamigrator.service.KafkaFlowMigrationService;
import org.apache.nifi.toolkit.kafkamigrator.service.KafkaTemplateMigrationService;
import org.apache.nifi.toolkit.kafkamigrator.MigratorConfiguration.MigratorConfigurationBuilder;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class KafkaMigrationServiceTest {

    private static final List<String> EXPECTED_CLASS_OR_TYPE_NAMES =
        Arrays.asList("org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_0",
                "org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_0",
                "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_0");

    private static final List<String> EXPECTED_ARTIFACTS =
            Arrays.asList("nifi-kafka-2-0-nar", "nifi-kafka-2-0-nar", "nifi-kafka-2-0-nar");

    private static final MigratorConfigurationBuilder CONFIGURATION_BUILDER =
            new MigratorConfigurationBuilder().setKafkaBrokers("kafkaBrokers, localhost:1234")
                                            .setTransaction(Boolean.FALSE);
    private static final XPath XPATH = XPathFactory.newInstance().newXPath();
    private static final String PATH_FOR_PROCESSORS_IN_FLOWS = ".//processor";
    private static final String PATH_FOR_PROCESSORS_IN_TEMPLATES = ".//processors";
    private static final String PATH_FOR_CLASS_ELEMENT = "class";
    private static final String PATH_FOR_TYPE_ELEMENT = "type";
    private static final String PATH_FOR_ARTIFACT_ELEMENT = "bundle/artifact";


    @Test
    public void testClassReplacement() throws XPathExpressionException, IOException {
        final KafkaFlowMigrationService kafkaMigrationService = new KafkaFlowMigrationService();
        final Document document = KafkaMigrationUtil.parseDocument();
        final List<String> originalClassNames = createClassResultList(document);

        kafkaMigrationService.replaceKafkaProcessors(document, CONFIGURATION_BUILDER);
        final List<String> actualClassNames = createClassResultList(document);

        assertSuccess(EXPECTED_CLASS_OR_TYPE_NAMES, actualClassNames, originalClassNames);
    }

    @Test
    public void testTypeReplacement() throws XPathExpressionException, IOException {
        final KafkaTemplateMigrationService kafkaMigrationService = new KafkaTemplateMigrationService();
        final Document document = KafkaMigrationUtil.parseDocument();
        final List<String> originalTypeNames = createTypeResultList(document);

        kafkaMigrationService.replaceKafkaProcessors(document, CONFIGURATION_BUILDER);
        final List<String> actualTypeNames = createTypeResultList(document);

        assertSuccess(EXPECTED_CLASS_OR_TYPE_NAMES, actualTypeNames, originalTypeNames);
    }

    @Test
    public void testArtifactReplacementInTemplate() throws XPathExpressionException, IOException {
        final KafkaTemplateMigrationService kafkaMigrationService = new KafkaTemplateMigrationService();
        final Document document = KafkaMigrationUtil.parseDocument();
        final List<String> originalArtifacts = createArtifactResultListForTemplate(document);

        kafkaMigrationService.replaceKafkaProcessors(document, CONFIGURATION_BUILDER);
        final List<String> actualArtifacts = createArtifactResultListForTemplate(document);

        assertSuccess(EXPECTED_ARTIFACTS, actualArtifacts, originalArtifacts);
    }

    @Test
    public void testArtifactReplacementInFlow() throws XPathExpressionException, IOException {
        final KafkaFlowMigrationService kafkaMigrationService = new KafkaFlowMigrationService();
        final Document document = KafkaMigrationUtil.parseDocument();
        final List<String> originalArtifacts = createArtifactResultListForFlow(document);

        kafkaMigrationService.replaceKafkaProcessors(document, CONFIGURATION_BUILDER);
        final List<String> actualArtifacts = createArtifactResultListForFlow(document);

        assertSuccess(EXPECTED_ARTIFACTS, actualArtifacts, originalArtifacts);
    }

    private List<String> createClassResultList(final Document document) throws XPathExpressionException {
        return createProcessorResultListForFlow(document, PATH_FOR_CLASS_ELEMENT);
    }

    private List<String> createArtifactResultListForFlow(final Document document) throws XPathExpressionException {
        return createProcessorResultListForFlow(document, PATH_FOR_ARTIFACT_ELEMENT);
    }

    private List<String> createTypeResultList(final Document document) throws XPathExpressionException {
        return createProcessorResultListForTemplate(document, PATH_FOR_TYPE_ELEMENT);
    }

    private List<String> createArtifactResultListForTemplate(final Document document) throws XPathExpressionException {
        return createProcessorResultListForTemplate(document, PATH_FOR_ARTIFACT_ELEMENT);
    }

    private List<String> createProcessorResultListForFlow(final Document document, final String elementPath) throws XPathExpressionException {
        return createProcessorResultList(document, PATH_FOR_PROCESSORS_IN_FLOWS, elementPath);
    }

    private List<String> createProcessorResultListForTemplate(final Document document, final String elementPath) throws XPathExpressionException {
        return createProcessorResultList(document, PATH_FOR_PROCESSORS_IN_TEMPLATES, elementPath);
    }

    private List<String> createProcessorResultList(final Document document, final String processorPath, final String elementPath) throws XPathExpressionException {
        final List<String> resultList = new ArrayList<>();
        final NodeList processors = (NodeList) XPATH.evaluate(processorPath, document, XPathConstants.NODESET);
        for (int i = 0; i < processors.getLength(); i++) {
            resultList.add(XPATH.evaluate(elementPath, processors.item(i)));
        }
        return resultList;
    }

    private void assertSuccess(final List<String> expectedArtifacts, final List<String> actualArtifacts, final List<String> originalArtifacts) {
        assertArrayEquals(expectedArtifacts.toArray(), actualArtifacts.toArray());
        assertNoReplacementHappened(originalArtifacts, actualArtifacts);
        assertReplacementHappened(originalArtifacts, actualArtifacts);
    }

    private void assertNoReplacementHappened(final List<String> originalArtifacts, final List<String> actualArtifacts) {
        assertEquals(originalArtifacts.get(0), actualArtifacts.get(0));
    }

    private void assertReplacementHappened(final List<String> originalArtifacts, final List<String> actualArtifacts) {
        assertNotEquals(originalArtifacts.get(1), actualArtifacts.get(1));
        assertNotEquals(originalArtifacts.get(2), actualArtifacts.get(2));
    }
}