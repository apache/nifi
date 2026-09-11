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

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.HashSet;
import java.util.Set;

public class ComponentAddedRebaseHandler implements RebaseHandler {

    private static final String NULL_COMPONENT_TYPE = "null";

    @Override
    public DifferenceType getSupportedType() {
        return DifferenceType.COMPONENT_ADDED;
    }

    @Override
    public RebaseAnalysis.ClassifiedDifference classify(final FlowDifference localDifference, final Set<FlowDifference> upstreamDifferences,
                                                        final VersionedProcessGroup targetSnapshot) {
        final VersionedComponent addedComponent = localDifference.getComponentB();
        if (!(addedComponent instanceof VersionedControllerService controllerService)) {
            final String componentType = addedComponent == null ? NULL_COMPONENT_TYPE : addedComponent.getClass().getSimpleName();
            return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE,
                    "Local component addition type %s is not supported for rebase".formatted(componentType));
        }

        final String parentGroupIdentifier = controllerService.getGroupIdentifier();
        if (parentGroupIdentifier == null) {
            return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, RebaseConflictCode.COMPONENT_NOT_FOUND,
                    "Controller Service %s does not specify a parent Process Group".formatted(controllerService.getIdentifier()));
        }

        final VersionedProcessGroup parentGroup = resolveParentGroup(targetSnapshot, parentGroupIdentifier, upstreamDifferences);
        if (parentGroup == null) {
            return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, RebaseConflictCode.COMPONENT_NOT_FOUND,
                    "Parent Process Group %s for Controller Service %s not found in target snapshot"
                            .formatted(parentGroupIdentifier, controllerService.getIdentifier()));
        }

        final VersionedComponent collidingComponent = RebaseHandlerUtils.findComponentById(targetSnapshot, controllerService.getIdentifier());
        if (collidingComponent != null) {
            return RebaseAnalysis.ClassifiedDifference.conflicting(localDifference, RebaseConflictCode.COMPONENT_IDENTIFIER_COLLISION,
                    "Target snapshot already contains component %s with identifier %s"
                            .formatted(collidingComponent.getClass().getSimpleName(), controllerService.getIdentifier()));
        }

        controllerService.setGroupIdentifier(parentGroup.getIdentifier());
        return RebaseAnalysis.ClassifiedDifference.compatible(localDifference);
    }

    @Override
    public void apply(final FlowDifference localDifference, final VersionedProcessGroup mergedFlow) {
        final VersionedControllerService controllerService = (VersionedControllerService) localDifference.getComponentB();
        final VersionedProcessGroup parentGroup = RebaseHandlerUtils.findProcessGroupById(mergedFlow, controllerService.getGroupIdentifier());
        if (parentGroup == null) {
            throw new IllegalStateException("Parent Process Group %s for Controller Service %s was verified during classification but is absent during apply"
                    .formatted(controllerService.getGroupIdentifier(), controllerService.getIdentifier()));
        }

        final VersionedComponent existingComponent = RebaseHandlerUtils.findComponentById(mergedFlow, controllerService.getIdentifier());
        if (existingComponent != null) {
            throw new IllegalStateException("Merged flow already contains component %s with identifier %s"
                    .formatted(existingComponent.getClass().getSimpleName(), controllerService.getIdentifier()));
        }

        final Set<VersionedControllerService> controllerServices = parentGroup.getControllerServices();
        if (controllerServices == null) {
            parentGroup.setControllerServices(new HashSet<>());
        }
        parentGroup.getControllerServices().add(controllerService);
    }

    private VersionedProcessGroup resolveParentGroup(final VersionedProcessGroup targetSnapshot, final String parentGroupIdentifier,
                                                     final Set<FlowDifference> upstreamDifferences) {
        final VersionedProcessGroup parentGroup = RebaseHandlerUtils.findProcessGroupById(targetSnapshot, parentGroupIdentifier);
        if (parentGroup != null) {
            return parentGroup;
        }

        final boolean parentRemoved = upstreamDifferences.stream()
                .filter(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_REMOVED)
                .map(FlowDifference::getComponentA)
                .filter(VersionedProcessGroup.class::isInstance)
                .map(VersionedComponent::getIdentifier)
                .anyMatch(parentGroupIdentifier::equals);

        return parentRemoved ? null : targetSnapshot;
    }

}
