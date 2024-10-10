/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.registry.flow.git.serialize;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.VersionedComponent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Jackson Module to customize serialization of versioned component objects.
 */
public class VersionedComponentModule extends SimpleModule {

    private static final Set<String> EXCLUDE_JSON_FIELDS = Set.of("instanceIdentifier", "instanceGroupId");

    @Override
    public void setupModule(final SetupContext context) {
        super.setupModule(context);
        context.addBeanSerializerModifier(new VersionedComponentBeanSerializerModifier());
    }

    private static class VersionedComponentBeanSerializerModifier extends BeanSerializerModifier {
        @Override
        public List<BeanPropertyWriter> changeProperties(final SerializationConfig config, final BeanDescription beanDesc, final List<BeanPropertyWriter> beanProperties) {
            if (!VersionedComponent.class.isAssignableFrom(beanDesc.getBeanClass())
                    && !ConnectableComponent.class.isAssignableFrom(beanDesc.getBeanClass())) {
                return super.changeProperties(config, beanDesc, beanProperties);
            }

            final List<BeanPropertyWriter> includedProperties = new ArrayList<>();
            for (final BeanPropertyWriter property : beanProperties) {
                if (!EXCLUDE_JSON_FIELDS.contains(property.getName())) {
                    includedProperties.add(property);
                }
            }
            return includedProperties;
        }
    }
}
