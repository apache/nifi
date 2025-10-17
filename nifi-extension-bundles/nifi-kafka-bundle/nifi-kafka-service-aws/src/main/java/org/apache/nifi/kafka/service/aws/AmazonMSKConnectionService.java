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
package org.apache.nifi.kafka.service.aws;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.kafka.shared.property.SaslMechanism;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

@Tags({"AWS", "MSK", "streaming", "kafka"})
@CapabilityDescription("Provides and manages connections to AWS MSK Kafka Brokers for producer or consumer operations.")
public class AmazonMSKConnectionService extends Kafka3ConnectionService {

    public static final PropertyDescriptor AWS_SASL_MECHANISM = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SASL_MECHANISM)
            .allowableValues(
                    SaslMechanism.AWS_MSK_IAM,
                    SaslMechanism.SCRAM_SHA_512
            )
            .defaultValue(SaslMechanism.AWS_MSK_IAM)
            .build();

    private final List<PropertyDescriptor> supportedPropertyDescriptors;

    public AmazonMSKConnectionService() {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());

        final ListIterator<PropertyDescriptor> descriptors = propertyDescriptors.listIterator();
        while (descriptors.hasNext()) {
            final PropertyDescriptor propertyDescriptor = descriptors.next();
            if (SASL_MECHANISM.equals(propertyDescriptor)) {
                descriptors.remove();
                // Add AWS MSK properties
                descriptors.add(AWS_SASL_MECHANISM);
                descriptors.add(KafkaClientComponent.AWS_ROLE_SOURCE);
                descriptors.add(KafkaClientComponent.AWS_PROFILE_NAME);
                descriptors.add(KafkaClientComponent.AWS_ASSUME_ROLE_ARN);
                descriptors.add(KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME);
            }
        }

        supportedPropertyDescriptors = propertyDescriptors;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedPropertyDescriptors;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        // For backward compatibility: if an AWS Profile Name was configured previously,
        // set AWS Role Source to SPECIFIED_PROFILE
        if (config.isPropertySet(KafkaClientComponent.AWS_PROFILE_NAME)) {
            config.setProperty(KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.SPECIFIED_PROFILE.name());
        }
    }
}
