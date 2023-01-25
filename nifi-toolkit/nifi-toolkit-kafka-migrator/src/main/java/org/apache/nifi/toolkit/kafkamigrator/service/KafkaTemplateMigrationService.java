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

import org.apache.nifi.toolkit.kafkamigrator.MigratorConfiguration.MigratorConfigurationBuilder;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorDescriptor;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorType;
import org.apache.nifi.toolkit.kafkamigrator.migrator.ConsumeKafkaTemplateMigrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.Migrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.PublishKafkaTemplateMigrator;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.TemplatePropertyXpathDescriptor;

public class KafkaTemplateMigrationService implements KafkaMigrationService {
    private static final String XPATH_FOR_PROCESSORS_IN_TEMPLATE = ".//processors";
    private static final String TYPE_TAG_NAME = "type";

    public KafkaTemplateMigrationService() {
    }

    @Override
    public String getPathForProcessors() {
        return XPATH_FOR_PROCESSORS_IN_TEMPLATE;
    }

    @Override
    public String getPathForClass() {
        return TYPE_TAG_NAME;
    }

    @Override
    public Migrator createPublishMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_NOT_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.PUBLISH))
                            .setPropertyXpathDescriptor(new TemplatePropertyXpathDescriptor(KafkaProcessorType.PUBLISH));
        return new PublishKafkaTemplateMigrator(configurationBuilder.build());
    }

    @Override
    public Migrator createConsumeMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_NOT_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.CONSUME))
                            .setPropertyXpathDescriptor(new TemplatePropertyXpathDescriptor(KafkaProcessorType.CONSUME));
        return new ConsumeKafkaTemplateMigrator(configurationBuilder.build());
    }

    @Override
    public Migrator createVersionEightPublishMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.PUBLISH))
                            .setPropertyXpathDescriptor(new TemplatePropertyXpathDescriptor(KafkaProcessorType.PUBLISH));
        return new PublishKafkaTemplateMigrator(configurationBuilder.build());
    }

    @Override
    public Migrator createVersionEightConsumeMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.CONSUME))
                            .setPropertyXpathDescriptor(new TemplatePropertyXpathDescriptor(KafkaProcessorType.CONSUME));
        return new ConsumeKafkaTemplateMigrator(configurationBuilder.build());
    }
}
