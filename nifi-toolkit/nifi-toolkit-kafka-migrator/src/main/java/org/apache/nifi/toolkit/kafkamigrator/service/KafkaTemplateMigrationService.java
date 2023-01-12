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

import org.apache.nifi.toolkit.kafkamigrator.migrator.ConsumeKafkaTemplateMigrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.Migrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.PublishKafkaTemplateMigrator;
import org.w3c.dom.Document;

import javax.xml.xpath.XPathExpressionException;
import java.util.Map;

public class KafkaTemplateMigrationService extends KafkaMigrationService {
    private static final String XPATH_FOR_PROCESSORS_IN_TEMPLATE = ".//processors";

    private static final String TYPE_TAG_NAME = "type";

    public KafkaTemplateMigrationService() {
        this.path = XPATH_FOR_PROCESSORS_IN_TEMPLATE;
        this.pathForClass = TYPE_TAG_NAME;
    }

    @Override
    public void replaceKafkaProcessors(final Document document, final Map<String, String> arguments) throws XPathExpressionException {
        replaceProcessors(document, arguments);
    }

    @Override
    protected Migrator createPublishMigrator(final Map<String, String> arguments) {
        return new PublishKafkaTemplateMigrator(arguments, IS_NOT_VERSION_EIGHT_PROCESSOR);
    }

    @Override
    protected Migrator createConsumeMigrator(final Map<String, String> arguments) {
        return new ConsumeKafkaTemplateMigrator(arguments, IS_NOT_VERSION_EIGHT_PROCESSOR);
    }

    @Override
    protected Migrator createVersionEightPublishMigrator(final Map<String, String> arguments) {
        return new PublishKafkaTemplateMigrator(arguments, IS_VERSION_EIGHT_PROCESSOR);
    }

    @Override
    protected Migrator createVersionEightConsumeMigrator(final Map<String, String> arguments) {
        return new ConsumeKafkaTemplateMigrator(arguments, IS_VERSION_EIGHT_PROCESSOR);
    }
}
