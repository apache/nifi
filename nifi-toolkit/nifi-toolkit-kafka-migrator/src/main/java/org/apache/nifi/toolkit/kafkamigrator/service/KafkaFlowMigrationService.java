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

import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorDescriptor;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.KafkaProcessorType;
import org.apache.nifi.toolkit.kafkamigrator.migrator.ConsumeKafkaFlowMigrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.Migrator;
import org.apache.nifi.toolkit.kafkamigrator.migrator.PublishKafkaFlowMigrator;
import org.apache.nifi.toolkit.kafkamigrator.MigratorConfiguration.MigratorConfigurationBuilder;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.FlowPropertyXpathDescriptor;


public class KafkaFlowMigrationService implements KafkaMigrationService {
    private static final String XPATH_FOR_PROCESSORS_IN_FLOW = ".//processor";
    private static final String CLASS_TAG_NAME = "class";

    public KafkaFlowMigrationService() {
    }

    @Override
    public String getPathForProcessors() {
        return XPATH_FOR_PROCESSORS_IN_FLOW;
    }

    @Override
    public String getPathForClass() {
        return CLASS_TAG_NAME;
    }

    @Override
    public Migrator createPublishMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_NOT_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.PUBLISH))
                            .setPropertyXpathDescriptor(new FlowPropertyXpathDescriptor(KafkaProcessorType.PUBLISH));
        return new PublishKafkaFlowMigrator(configurationBuilder.build());
    }

    @Override
    public Migrator createConsumeMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_NOT_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.CONSUME))
                            .setPropertyXpathDescriptor(new FlowPropertyXpathDescriptor(KafkaProcessorType.CONSUME));
        return new ConsumeKafkaFlowMigrator(configurationBuilder.build());
    }

    @Override
    public Migrator createVersionEightPublishMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.PUBLISH))
                            .setPropertyXpathDescriptor(new FlowPropertyXpathDescriptor(KafkaProcessorType.PUBLISH));
        return new PublishKafkaFlowMigrator(configurationBuilder.build());
    }

    @Override
    public Migrator createVersionEightConsumeMigrator(final MigratorConfigurationBuilder configurationBuilder) {
        configurationBuilder.setIsVersion8Processor(IS_VERSION_EIGHT_PROCESSOR)
                            .setProcessorDescriptor(new KafkaProcessorDescriptor(KafkaProcessorType.CONSUME))
                            .setPropertyXpathDescriptor(new FlowPropertyXpathDescriptor(KafkaProcessorType.CONSUME));
        return new ConsumeKafkaFlowMigrator(configurationBuilder.build());
    }
}
