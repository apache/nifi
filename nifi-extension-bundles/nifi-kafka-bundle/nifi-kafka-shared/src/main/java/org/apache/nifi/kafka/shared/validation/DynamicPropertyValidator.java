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
package org.apache.nifi.kafka.shared.validation;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.kafka.shared.property.provider.KafkaPropertyNameProvider;
import org.apache.nifi.kafka.shared.property.provider.StandardKafkaPropertyNameProvider;

import java.util.Set;

/**
 * Validator for dynamic Kafka properties
 */
public class DynamicPropertyValidator implements Validator {
    private static final String PARTITIONS_PROPERTY_PREFIX = "partitions";

    private final Set<String> clientPropertyNames;

    public DynamicPropertyValidator(final Class<?> kafkaClientClass) {
        final KafkaPropertyNameProvider provider = new StandardKafkaPropertyNameProvider(kafkaClientClass);
        clientPropertyNames = provider.getPropertyNames();
    }

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder builder = new ValidationResult.Builder();
        builder.subject(subject);

        if (subject.startsWith(PARTITIONS_PROPERTY_PREFIX)) {
            builder.valid(true);
        } else {
            final boolean valid = clientPropertyNames.contains(subject);
            builder.valid(valid);
            builder.explanation("must be a known Kafka client configuration property");
        }

        return builder.build();
    }
}
