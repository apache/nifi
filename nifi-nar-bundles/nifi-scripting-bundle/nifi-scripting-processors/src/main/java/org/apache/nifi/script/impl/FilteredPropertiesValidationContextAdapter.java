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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;

/**
 * filter properties in the ValidationContext, proxy approach, for removing unwanted properties
 *
 * @author pfreydiere
 *
 */
public class FilteredPropertiesValidationContextAdapter extends ValidationContextAdapter {

    private Set<PropertyDescriptor> removedProperties;
    private Set<String> removedPropertyNames;

    public FilteredPropertiesValidationContextAdapter(ValidationContext validationContext, Set<PropertyDescriptor> removedProperties) {
        super(validationContext);
        this.removedProperties = removedProperties;
        Set<String> keys = new HashSet<>();
        for (PropertyDescriptor p : removedProperties) {
            keys.add(p.getName());
        }
        this.removedPropertyNames = keys;
    }

    @Override
    public Map<String, String> getAllProperties() {
        HashMap<String, String> returnedProperties = new HashMap<>(super.getAllProperties());

        int i = 0;
        ArrayList<String> keys = new ArrayList<String>(returnedProperties.keySet());
        while (i < keys.size()) {
            String k = keys.get(i);
            if (removedPropertyNames.contains(k)) {
                keys.remove(i);
                returnedProperties.remove(k);
            } else {
                i++;
            }
        }

        return returnedProperties;
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {

        Map<PropertyDescriptor, String> originalProperties = super.getProperties();
        Map<PropertyDescriptor, String> returnedProperties = new HashMap<>();
        for (PropertyDescriptor p : removedProperties) {
            if (!originalProperties.containsKey(p)) {
                returnedProperties.put(p, originalProperties.get(p));
            }
        }

        return returnedProperties;
    }

    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
        if (!removedProperties.contains(descriptor)) {
            return super.getProperty(descriptor);
        }

        return null;
    }

}
