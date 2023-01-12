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

import org.apache.nifi.toolkit.kafkamigrator.migrator.ConsumeKafkaFlowMigrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.Migrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.PublishKafkaFlowMigrator;
import org.w3c.dom.Document;

import javax.xml.xpath.XPathExpressionException;
import java.util.Map;

public class KafkaFlowMigrationService extends KafkaMigrationService {
    private static final String XPATH_FOR_PROCESSORS_IN_FLOW = ".//processor";
    private static final String CLASS_TAG_NAME = "class";

    public KafkaFlowMigrationService() {
        this.path = XPATH_FOR_PROCESSORS_IN_FLOW;
        this.pathForClass = CLASS_TAG_NAME;
    }

    @Override
    public void replaceKafkaProcessors(final Document document, final Map<String, String> arguments) throws XPathExpressionException {
        replaceProcessors(document, arguments);
    }

    @Override
    protected Migrator createPublishMigrator(final Map<String, String> arguments) {
        return new PublishKafkaFlowMigrator(arguments, IS_NOT_VERSION_EIGHT_PROCESSOR);
    }

    @Override
    protected Migrator createConsumeMigrator(final Map<String, String> arguments) {
        return new ConsumeKafkaFlowMigrator(arguments, IS_NOT_VERSION_EIGHT_PROCESSOR);
    }

    @Override
    protected Migrator createVersionEightPublishMigrator(final Map<String, String> arguments) {
        return new PublishKafkaFlowMigrator(arguments, IS_VERSION_EIGHT_PROCESSOR);
    }

    @Override
    protected Migrator createVersionEightConsumeMigrator(final Map<String, String> arguments) {
        return new ConsumeKafkaFlowMigrator(arguments, IS_VERSION_EIGHT_PROCESSOR);
    }
}
