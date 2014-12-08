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
package org.apache.nifi.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;

public class ControllerServiceConfiguration {

    private final ControllerService service;
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private String annotationData;
    private Map<PropertyDescriptor, String> properties = new HashMap<>();

    public ControllerServiceConfiguration(final ControllerService service) {
        this.service = service;
    }

    public ControllerService getService() {
        return service;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled.set(enabled);
    }

    public boolean isEnabled() {
        return this.enabled.get();
    }

    public void setProperties(final Map<PropertyDescriptor, String> props) {
        this.properties = new HashMap<>(props);
    }

    public String getProperty(final PropertyDescriptor descriptor) {
        final String value = properties.get(descriptor);
        if (value == null) {
            return descriptor.getDefaultValue();
        } else {
            return value;
        }
    }

    public void setAnnotationData(final String annotationData) {
        this.annotationData = annotationData;
    }

    public String getAnnotationData() {
        return annotationData;
    }

    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }
}
