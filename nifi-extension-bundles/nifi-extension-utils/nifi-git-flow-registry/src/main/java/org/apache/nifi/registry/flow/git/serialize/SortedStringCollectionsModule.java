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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.type.CollectionType;

import java.util.List;
import java.util.Set;

public class SortedStringCollectionsModule extends SimpleModule {

    final Set<String> fieldsToSkipSorting = Set.of("inheritedParameterContexts");

    @Override
    public void setupModule(final SetupContext context) {
        super.setupModule(context);
        context.addBeanSerializerModifier(new BeanSerializerModifier() {
            @Override
            public JsonSerializer<?> modifyCollectionSerializer(final SerializationConfig config,
                    final CollectionType valueType,
                    final BeanDescription beanDesc,
                    final JsonSerializer<?> serializer) {
                // Only apply to List<String>
                if (List.class.isAssignableFrom(valueType.getRawClass()) && valueType.getContentType().getRawClass() == String.class) {
                    return new SortedStringListSerializer((JsonSerializer<Object>) serializer, fieldsToSkipSorting);
                }
                // Only apply to Set<String>
                if (Set.class.isAssignableFrom(valueType.getRawClass()) && valueType.getContentType().getRawClass() == String.class) {
                    return new SortedStringSetSerializer();
                }
                // Only apply to set of enums
                if (Set.class.isAssignableFrom(valueType.getRawClass()) && valueType.getContentType().isEnumType()) {
                    return new SortedEnumSetSerializer();
                }
                return serializer;
            }
        });
    }
}

