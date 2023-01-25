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
import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorType;
import org.apache.nifi.toolkit.kafkamigrator.migrator.Migrator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.nifi.toolkit.kafkamigrator.MigratorConfiguration.MigratorConfigurationBuilder;

public interface KafkaMigrationService {

     String REGEX_FOR_REPLACEABLE_PROCESSOR_NAMES = "(Get|Put|Consume|Publish)Kafka(Record)?(_0_1\\d)?";
     boolean IS_VERSION_EIGHT_PROCESSOR = Boolean.TRUE;
     boolean IS_NOT_VERSION_EIGHT_PROCESSOR = Boolean.FALSE;

    String getPathForProcessors();
    String getPathForClass();
    Migrator createPublishMigrator(final MigratorConfigurationBuilder configurationBuilder);
    Migrator createConsumeMigrator(final MigratorConfigurationBuilder configurationBuilder);
    Migrator createVersionEightPublishMigrator(final MigratorConfigurationBuilder configurationBuilder);
    Migrator createVersionEightConsumeMigrator(final MigratorConfigurationBuilder configurationBuilder);

    default void replaceKafkaProcessors(final Document document, final MigratorConfigurationBuilder configurationBuilder) throws XPathExpressionException {
        Migrator migrator;
        final XPath xPath = XPathFactory.newInstance().newXPath();

        final NodeList processors = (NodeList) xPath.evaluate(getPathForProcessors(), document, XPathConstants.NODESET);
        for (int i = 0; i < processors.getLength(); i++) {
            final Node processor = processors.item(i);
            final Element className = ((Element) xPath.evaluate(getPathForClass(), processor, XPathConstants.NODE));
            final String processorName = StringUtils.substringAfterLast(className.getTextContent(), ".");

            if (processorName.matches(REGEX_FOR_REPLACEABLE_PROCESSOR_NAMES)) {
                if (processorName.contains(KafkaProcessorType.PUBLISH.getProcessorType())) {
                    migrator = createPublishMigrator(configurationBuilder);
                } else if (processorName.contains(KafkaProcessorType.PUT.getProcessorType())) {
                    migrator = createVersionEightPublishMigrator(configurationBuilder);
                } else if (processorName.contains(KafkaProcessorType.CONSUME.getProcessorType())) {
                    migrator = createConsumeMigrator(configurationBuilder);
                } else {
                    migrator = createVersionEightConsumeMigrator(configurationBuilder);
                }

                migrator.migrate(className, processor);
            }
        }
    }
}
