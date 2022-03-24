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

import java.util.Set

import org.apache.nifi.controller.ControllerServiceInitializationContext
import org.apache.nifi.reporting.InitializationException


class GroovyLookupService implements LookupService<String> {

    def lookupTable = [
            'Hello': 'Hi',
            'World': 'there'
    ]


    @Override
    Optional<String> lookup(Map<String, String> coordinates) {
        final String key = coordinates.values().iterator().next();
        Optional.ofNullable(lookupTable[key])
    }
    
    Set<String> getRequiredKeys() {
        return java.util.Collections.emptySet();
    }
    
    @Override
    Class<?> getValueType() {
        return String
    }

    @Override
    void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    @Override
    Collection<ValidationResult> validate(ValidationContext context) {
        return null
    }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        return null
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

    }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        return null
    }

    @Override
    String getIdentifier() {
        return null
    }
}

lookupService = new GroovyLookupService()