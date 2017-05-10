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

package org.apache.nifi.lookup;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

@Tags({"lookup", "enrich", "key", "value"})
@CapabilityDescription("Allows users to add key/value pairs as User-defined Properties. Each property that is added can be looked up by Property Name.")
public class SimpleKeyValueLookupService extends AbstractControllerService implements StringLookupService {
    private volatile Map<String, String> lookupValues = new HashMap<>();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .dynamic(true)
            .addValidator(Validator.VALID)
            .build();
    }

    @OnEnabled
    public void cacheConfiguredValues(final ConfigurationContext context) {
        lookupValues = context.getProperties().entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getName(), entry -> context.getProperty(entry.getKey()).getValue()));
    }

    @Override
    public Optional<String> lookup(final String key) {
        return Optional.ofNullable(lookupValues.get(key));
    }
}
