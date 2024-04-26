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
package org.apache.nifi.kafka.shared.property.provider;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation of Kafka Property Name Provider using class name references from Kafka library
 */
public class StandardKafkaPropertyNameProvider implements KafkaPropertyNameProvider {
    private static final String COMMON_CLIENT_CONFIGS_CLASS = "org.apache.kafka.clients.CommonClientConfigs";

    private static final String SASL_CONFIGS_CLASS = "org.apache.kafka.common.config.SaslConfigs";

    private static final String SSL_CONFIGS_CLASS = "org.apache.kafka.common.config.SslConfigs";

    private static final String[] PROPERTY_CLASSES = new String[]{
            COMMON_CLIENT_CONFIGS_CLASS,
            SASL_CONFIGS_CLASS,
            SSL_CONFIGS_CLASS
    };

    private static final Pattern PROPERTY_PATTERN = Pattern.compile("^\\S+$");

    private final Set<String> propertyNames;

    public StandardKafkaPropertyNameProvider(final Class<?> kafkaClientClass) {
        final Set<String> kafkaClientPropertyNames = getStaticStringPropertyNames(kafkaClientClass);
        kafkaClientPropertyNames.addAll(getCommonPropertyNames());
        propertyNames = kafkaClientPropertyNames;
    }

    @Override
    public Set<String> getPropertyNames() {
        return propertyNames;
    }

    private static Set<String> getCommonPropertyNames() {
        final Set<String> propertyNames = new LinkedHashSet<>();

        for (final String propertyClassName : PROPERTY_CLASSES) {
            final Optional<Class<?>> propertyClassFound = findClass(propertyClassName);
            if (propertyClassFound.isPresent()) {
                final Class<?> propertyClass = propertyClassFound.get();
                final Set<String> classPropertyNames = getStaticStringPropertyNames(propertyClass);
                propertyNames.addAll(classPropertyNames);
            }
        }

        return propertyNames;
    }

    private static Set<String> getStaticStringPropertyNames(final Class<?> propertyClass) {
        final Set<String> propertyNames = new LinkedHashSet<>();

        for (final Field field : propertyClass.getDeclaredFields()) {
            final int modifiers = field.getModifiers();
            final Class<?> fieldType = field.getType();
            if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && String.class.equals(fieldType)) {
                final String fieldValue = getStaticFieldValue(field);
                final Matcher propertyMatcher = PROPERTY_PATTERN.matcher(fieldValue);
                if (propertyMatcher.matches()) {
                    propertyNames.add(fieldValue);
                }
            }
        }

        return propertyNames;
    }

    private static String getStaticFieldValue(final Field field) {
        try {
            return String.valueOf(field.get(null));
        } catch (final Exception e) {
            final String message = String.format("Unable to read Kafka Configuration class field [%s]", field.getName());
            throw new IllegalArgumentException(message, e);
        }
    }

    private static Optional<Class<?>> findClass(final String className) {
        try {
            return Optional.of(Class.forName(className));
        } catch (final ClassNotFoundException e) {
            return Optional.empty();
        }
    }
}
