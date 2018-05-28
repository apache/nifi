/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.util;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Helper method for operate with {@link PropertyValue}.
 * @see PropertyValue
 * @see PropertyValue#getValue()
 */
public final class PropertyValueUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyValueUtils.class);

    private PropertyValueUtils() {
    }

    @NonNull
    public static <E extends Enum> E getEnumValue(@NonNull final PropertyDescriptor propertyDescriptor,
                                                  @NonNull final PropertyContext context,
                                                  @NonNull final Class<E> enumType,
                                                  @NonNull final E defaultValue) {

        Objects.requireNonNull(propertyDescriptor, "PropertyDescriptor is required");
        Objects.requireNonNull(context, "PropertyContext is required");
        Objects.requireNonNull(enumType, "Enum type is required");
        Objects.requireNonNull(defaultValue, "Default value is required");

        PropertyValue property = context.getProperty(propertyDescriptor);
        if (property == null) {
            return defaultValue;
        }

        String propertyValue = property.getValue();

        Objects.requireNonNull(enumType, "Type of Enum is required");
        Objects.requireNonNull(defaultValue, "Default value which will be used if propertyValue is not enum value");

        E value = null;
        try {
            // avoid incompatible types:
            // inference variable E has incompatible upper bounds java.lang.Enum<E>,E
            //
            //noinspection RedundantCast
            value = propertyValue != null ? (E) Enum.valueOf(enumType, propertyValue) : null;

        } catch (IllegalArgumentException e) {

            LOG.debug("The specified enum type '{}' has no constant '{}'", enumType, propertyValue);
        }

        return value != null ? value : defaultValue;
    }

    @NonNull
    public static List<String> getList(@NonNull final PropertyDescriptor propertyDescriptor,
                                       @NonNull final PropertyContext context,
                                       @Nullable final FlowFile flowFile) {

        Objects.requireNonNull(propertyDescriptor, "PropertyDescriptor is required");
        Objects.requireNonNull(context, "PropertyContext is required");

        List<String> results = new ArrayList<>();

        PropertyValue property = context.getProperty(propertyDescriptor);
        if (property == null) {
            return results;
        }

        String value = property.evaluateAttributeExpressions(flowFile).getValue();
        if (value == null || value.trim().isEmpty()) {
            return results;
        }

        for (final String item : value.split(",")) {

            if (item != null && !item.trim().isEmpty()) {
                results.add(item.trim());
            }
        }

        return results;
    }

}
