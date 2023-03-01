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

import org.apache.nifi.toolkit.kafkamigrator.descriptor.ProcessorDescriptor;
import org.apache.nifi.toolkit.kafkamigrator.descriptor.PropertyXpathDescriptor;

public class MigratorConfiguration {
    final private String kafkaBrokers;
    final private boolean transaction;
    final private boolean isVersion8Processor;
    final private ProcessorDescriptor processorDescriptor;
    final private PropertyXpathDescriptor propertyXpathDescriptor;

    public MigratorConfiguration(final String kafkaBrokers, final boolean transaction, final boolean isVersion8Processor,
                                 final ProcessorDescriptor processorDescriptor, final PropertyXpathDescriptor propertyXpathDescriptor) {
        this.kafkaBrokers = kafkaBrokers;
        this.transaction = transaction;
        this.isVersion8Processor = isVersion8Processor;
        this.processorDescriptor = processorDescriptor;
        this.propertyXpathDescriptor = propertyXpathDescriptor;
    }

    public String getKafkaBrokers() {
        return kafkaBrokers;
    }

    public boolean isTransaction() {
        return transaction;
    }

    public boolean isVersion8Processor() {
        return isVersion8Processor;
    }

    public ProcessorDescriptor getProcessorDescriptor() {
        return processorDescriptor;
    }

    public PropertyXpathDescriptor getPropertyXpathDescriptor() {
        return propertyXpathDescriptor;
    }

    public static class MigratorConfigurationBuilder {
        private String kafkaBrokers;
        private boolean transaction;
        private boolean isVersion8Processor;
        private ProcessorDescriptor processorDescriptor;
        private PropertyXpathDescriptor propertyXpathDescriptor;

        public MigratorConfigurationBuilder setKafkaBrokers(final String kafkaBrokers) {
            this.kafkaBrokers = kafkaBrokers;
            return this;
        }

        public MigratorConfigurationBuilder setTransaction(final boolean transaction) {
            this.transaction = transaction;
            return this;
        }

        public MigratorConfigurationBuilder setIsVersion8Processor(final boolean isVersion8Processor) {
            this.isVersion8Processor = isVersion8Processor;
            return this;
        }

        public MigratorConfigurationBuilder setProcessorDescriptor(final ProcessorDescriptor processorDescriptor) {
            this.processorDescriptor = processorDescriptor;
            return this;
        }

        public MigratorConfigurationBuilder setPropertyXpathDescriptor(final PropertyXpathDescriptor propertyXpathDescriptor) {
            this.propertyXpathDescriptor = propertyXpathDescriptor;
            return this;
        }

        public MigratorConfiguration build() {
            return new MigratorConfiguration(kafkaBrokers, transaction, isVersion8Processor,
                                            processorDescriptor, propertyXpathDescriptor);
        }
    }
}
