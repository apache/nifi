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
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropertyDescriptorDtoMerger {
    public static void merge(PropertyDescriptorDTO clientPropertyDescriptor, Map<NodeIdentifier, PropertyDescriptorDTO> dtoMap) {
        final Map<Integer, List<AllowableValueEntity>> allowableValueMap = new HashMap<>();

        // values are guaranteed to be in order, so map each allowable value for each property descriptor across all node IDs to the index of the value in the descriptor's list of allowable values
        for (final Map.Entry<NodeIdentifier, PropertyDescriptorDTO> nodeEntry : dtoMap.entrySet()) {
            final PropertyDescriptorDTO nodePropertyDescriptor = nodeEntry.getValue();
            final List<AllowableValueEntity> nodePropertyDescriptorAllowableValues = nodePropertyDescriptor.getAllowableValues();
            if (nodePropertyDescriptorAllowableValues != null) {
                nodePropertyDescriptorAllowableValues.stream().forEach(allowableValueEntity -> {
                    allowableValueMap.computeIfAbsent(nodePropertyDescriptorAllowableValues.indexOf(allowableValueEntity), propertyDescriptorToAllowableValue -> new ArrayList<>())
                            .add(allowableValueEntity);
                });
            }
        }

        // for each AllowableValueEntity in this PropertyDescriptorDTO, get the corresponding AVs previously aggregated and merge them.
        final List<AllowableValueEntity> clientPropertyDescriptorAllowableValues = clientPropertyDescriptor.getAllowableValues();
        if (clientPropertyDescriptorAllowableValues != null) {
            for (AllowableValueEntity clientAllowableValueEntity : clientPropertyDescriptorAllowableValues) {
                AllowableValueEntityMerger.merge(clientAllowableValueEntity, allowableValueMap.get(clientPropertyDescriptorAllowableValues.indexOf(clientAllowableValueEntity)));
            }
        }
    }
}
