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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedConfigurableComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PropertyChangedRebaseHandler implements RebaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(PropertyChangedRebaseHandler.class);

    @Override
    public DifferenceType getSupportedType() {
        return DifferenceType.PROPERTY_CHANGED;
    }

    @Override
    public RebaseAnalysis.ClassifiedDifference classify(final FlowDifference localDifference, final Set<FlowDifference> upstreamDifferences,
                                                        final VersionedProcessGroup targetSnapshot) {
        final String propertyName = localDifference.getFieldName().orElse(null);
        if (propertyName == null) {
            return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, "MISSING_FIELD_NAME",
                    "Property change difference does not specify a field name");
        }

        final String componentIdentifier = localDifference.getComponentB().getIdentifier();

        for (final FlowDifference upstreamDifference : upstreamDifferences) {
            if (upstreamDifference.getDifferenceType() == DifferenceType.PROPERTY_CHANGED
                    && componentIdentifier.equals(upstreamDifference.getComponentA().getIdentifier())
                    && upstreamDifference.getFieldName().isPresent()
                    && propertyName.equals(upstreamDifference.getFieldName().get())) {
                if (Objects.equals(localDifference.getValueB(), upstreamDifference.getValueB())) {
                    return RebaseAnalysis.ClassifiedDifference.compatible(localDifference);
                }
                return RebaseAnalysis.ClassifiedDifference.conflicting(localDifference, "SAME_PROPERTY",
                        "Both local and upstream flows modified property '%s' on component %s".formatted(propertyName, componentIdentifier));
            }
        }

        if (localDifference.getValueA() == null) {
            final VersionedConfigurableComponent targetComponent = RebaseHandlerUtils.findConfigurableComponentById(targetSnapshot, componentIdentifier);
            if (targetComponent == null) {
                return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, "COMPONENT_NOT_FOUND",
                        "Component %s not found in target snapshot".formatted(componentIdentifier));
            }
            final Map<String, VersionedPropertyDescriptor> descriptors = targetComponent.getPropertyDescriptors();
            if (descriptors == null || !descriptors.containsKey(propertyName)) {
                return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, "DESCRIPTOR_CHANGED",
                        "Property descriptor '%s' no longer exists on component %s".formatted(propertyName, componentIdentifier));
            }
            final VersionedPropertyDescriptor descriptor = descriptors.get(propertyName);
            if (!descriptor.isSensitive()) {
                return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, "DESCRIPTOR_CHANGED",
                        "Property '%s' on component %s is no longer sensitive".formatted(propertyName, componentIdentifier));
            }
        }

        return RebaseAnalysis.ClassifiedDifference.compatible(localDifference);
    }

    @Override
    public void apply(final FlowDifference localDifference, final VersionedProcessGroup mergedFlow) {
        final String propertyName = localDifference.getFieldName().orElse(null);
        if (propertyName == null) {
            return;
        }

        final String componentIdentifier = localDifference.getComponentB().getIdentifier();
        final VersionedConfigurableComponent component = RebaseHandlerUtils.findConfigurableComponentById(mergedFlow, componentIdentifier);
        if (component == null) {
            logger.warn("Unable to apply property change: configurable component [{}] not found in merged flow", componentIdentifier);
            return;
        }

        final Map<String, String> existingProperties = component.getProperties();
        final Map<String, String> updatedProperties = existingProperties != null ? new HashMap<>(existingProperties) : new HashMap<>();
        updatedProperties.put(propertyName, (String) localDifference.getValueB());
        component.setProperties(updatedProperties);
    }
}
