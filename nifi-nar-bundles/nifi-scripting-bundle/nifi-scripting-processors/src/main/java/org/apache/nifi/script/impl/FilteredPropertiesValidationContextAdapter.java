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
package org.apache.nifi.script.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;

/**
 * Filters properties in the ValidationContext, proxy approach, for removing unwanted properties
 *
 */
public class FilteredPropertiesValidationContextAdapter extends ValidationContextAdapter {

    private HashMap<PropertyDescriptor, String> properties;

    public FilteredPropertiesValidationContextAdapter(ValidationContext validationContext, Set<PropertyDescriptor> removedProperties) {
        super(validationContext);
        properties = new HashMap<>();
        Map<PropertyDescriptor, String> parentProperties = super.getProperties();
        if (parentProperties != null) {
            for (Map.Entry<PropertyDescriptor, String> propertyEntry: parentProperties.entrySet()) {
                if (!removedProperties.contains(propertyEntry.getKey())) {
                    properties.put(propertyEntry.getKey(), propertyEntry.getValue());
                }
            }
        }
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String,String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
        if (properties.keySet().contains(descriptor)) {
            return super.getProperty(descriptor);
        }
        return null;
    }

}
