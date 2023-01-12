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
package org.apache.nifi.toolkit.kafkamigrator.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.kafkamigrator.migrator.Migrator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.Map;

public class KafkaMigrationService {

    private final static String REGEX_FOR_REPLACEABLE_PROCESSOR_NAMES = "(Get|Put|Consume|Publish)Kafka(Record)?(_0_1\\d)?";
    private final static String NEW_KAFKA_PROCESSOR_VERSION = "_2_0";
    private final static String ARTIFACT = "nifi-kafka-2-0-nar";
    private final static String PATH_FOR_ARTIFACT = "bundle/artifact";
    protected static final boolean IS_VERSION_EIGHT_PROCESSOR = Boolean.TRUE;
    protected static final boolean IS_NOT_VERSION_EIGHT_PROCESSOR = Boolean.FALSE;

    protected String path;
    protected String pathForClass;

    public void replaceKafkaProcessors(final Document document, final Map<String, String> arguments) throws XPathExpressionException {
        final KafkaFlowMigrationService flowMigrationService = new KafkaFlowMigrationService();
        final KafkaTemplateMigrationService templateMigrationService = new KafkaTemplateMigrationService();

        flowMigrationService.replaceKafkaProcessors(document, arguments);
        templateMigrationService.replaceKafkaProcessors(document, arguments);
    }

    protected void replaceProcessors(Document document, Map<String, String> arguments) throws XPathExpressionException {
        Migrator migrator;
        final XPath xPath = XPathFactory.newInstance().newXPath();

        final NodeList processors = (NodeList) xPath.evaluate(path, document, XPathConstants.NODESET);
        for (int i = 0; i < processors.getLength(); i++) {
            final Node processor = processors.item(i);
            final String className = xPath.evaluate(pathForClass, processor);
            final String processorName = StringUtils.substringAfterLast(className, ".");

            if (replaceableKafkaProcessor(processorName)) {
                if (processorName.contains("Publish")) {
                    migrator = createPublishMigrator(arguments);
                } else if (processorName.contains("Put")) {
                    migrator = createVersionEightPublishMigrator(arguments);
                } else if (processorName.contains("Consume")) {
                    migrator = createConsumeMigrator(arguments);
                } else {
                    migrator = createVersionEightConsumeMigrator(arguments);
                }

                migrator.configureProperties(processor);
                migrator.configureComponentSpecificSteps(processor, arguments);
                migrator.configureDescriptors(processor);

                final String newClassName = replaceClassNameWithNewProcessorName(className, processorName);
                ((Element) xPath.evaluate(pathForClass, processor, XPathConstants.NODE)).setTextContent(newClassName);
                ((Element) xPath.evaluate(PATH_FOR_ARTIFACT, processor, XPathConstants.NODE)).setTextContent(ARTIFACT);
            }
        }
    }

    protected Migrator createPublishMigrator(final Map<String, String> arguments) {
        return null;
    }

    protected Migrator createConsumeMigrator(final Map<String, String> arguments) {
        return null;
    }

    protected Migrator createVersionEightPublishMigrator(final Map<String, String> arguments) {
        return null;
    }

    protected Migrator createVersionEightConsumeMigrator(final Map<String, String> arguments) {
        return null;
    }

    private static boolean replaceableKafkaProcessor(final String processorName) {
        return processorName.matches(REGEX_FOR_REPLACEABLE_PROCESSOR_NAMES);
    }

    private static String replaceClassNameWithNewProcessorName(final String className, final String processorName) {
        final String newProcessorName = StringUtils.replaceEach(processorName, new String[]{"Get", "Put"}, new String[]{"pubsub.Consume", "pubsub.Publish"});
        final String processorNameWithNewVersion =
                newProcessorName.replaceFirst("$|_0_1\\d?", NEW_KAFKA_PROCESSOR_VERSION);
        return StringUtils.replace(className, processorName, processorNameWithNewVersion);
    }
}
