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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.AllowableValueDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PropertyDescriptorDtoMerger {
    public static void merge(PropertyDescriptorDTO clientPropertyDescriptor, Map<NodeIdentifier, PropertyDescriptorDTO> dtoMap) {
        if (clientPropertyDescriptor.getAllowableValues() != null) {
            Map<AllowableValueDTO, List<AllowableValueEntity>> allowableValueDtoToEntities = new LinkedHashMap<>();

            addEntities(clientPropertyDescriptor, allowableValueDtoToEntities);
            dtoMap.values().forEach(propertyDescriptorDTO -> addEntities(propertyDescriptorDTO, allowableValueDtoToEntities));

            List<AllowableValueEntity> mergedAllowableValues = allowableValueDtoToEntities.values().stream()
                .filter(entities -> Integer.valueOf(entities.size()).equals(dtoMap.size() + 1))
                .map(AllowableValueEntityMerger::merge)
                .collect(Collectors.toList());

            clientPropertyDescriptor.setAllowableValues(mergedAllowableValues);
        }
    }

    private static void addEntities(PropertyDescriptorDTO propertyDescriptorDTO, Map<AllowableValueDTO, List<AllowableValueEntity>> dtoToEntities) {
        propertyDescriptorDTO.getAllowableValues().forEach(
            allowableValueEntity -> dtoToEntities.computeIfAbsent(allowableValueEntity.getAllowableValue(), __ -> new ArrayList<>()).add(allowableValueEntity)
        );
    }
}
