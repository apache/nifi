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
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

/**
 * Handles scheduling strategy and run schedule changes on processors. The two fields are evaluated together because a run
 * schedule is only meaningful for its scheduling strategy: an upstream change to either field conflicts with the local change
 * unless it results in the same value as the local processor.
 */
public class SchedulingChangedRebaseHandler implements RebaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(SchedulingChangedRebaseHandler.class);

    private final DifferenceType supportedType;

    public SchedulingChangedRebaseHandler(final DifferenceType supportedType) {
        if (supportedType != DifferenceType.SCHEDULING_STRATEGY_CHANGED && supportedType != DifferenceType.RUN_SCHEDULE_CHANGED) {
            throw new IllegalArgumentException("Unsupported difference type " + supportedType);
        }
        this.supportedType = supportedType;
    }

    @Override
    public DifferenceType getSupportedType() {
        return supportedType;
    }

    @Override
    public RebaseAnalysis.ClassifiedDifference classify(final FlowDifference localDifference, final Set<FlowDifference> upstreamDifferences,
                                                        final VersionedProcessGroup targetSnapshot) {
        final VersionedComponent localComponent = localDifference.getComponentB();
        final String componentIdentifier = localComponent.getIdentifier();
        if (!(localComponent instanceof VersionedProcessor localProcessor)) {
            return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE,
                    "Scheduling change is not supported for component %s of type %s".formatted(componentIdentifier, localComponent.getComponentType()));
        }

        for (final FlowDifference upstreamDifference : upstreamDifferences) {
            if (upstreamDifference.getComponentA() == null || !componentIdentifier.equals(upstreamDifference.getComponentA().getIdentifier())) {
                continue;
            }

            final String localValue;
            if (upstreamDifference.getDifferenceType() == DifferenceType.SCHEDULING_STRATEGY_CHANGED) {
                localValue = localProcessor.getSchedulingStrategy();
            } else if (upstreamDifference.getDifferenceType() == DifferenceType.RUN_SCHEDULE_CHANGED) {
                localValue = localProcessor.getSchedulingPeriod();
            } else {
                continue;
            }

            if (!Objects.equals(localValue, upstreamDifference.getValueB())) {
                return RebaseAnalysis.ClassifiedDifference.conflicting(localDifference, RebaseConflictCode.SAME_SCHEDULING,
                        "Both local and upstream flows modified the scheduling of processor " + componentIdentifier);
            }
        }
        return RebaseAnalysis.ClassifiedDifference.compatible(localDifference);
    }

    @Override
    public void apply(final FlowDifference localDifference, final VersionedProcessGroup mergedFlow) {
        final String componentIdentifier = localDifference.getComponentB().getIdentifier();
        if (!(RebaseHandlerUtils.findComponentById(mergedFlow, componentIdentifier) instanceof VersionedProcessor processor)) {
            logger.warn("Unable to apply scheduling change: processor [{}] not found in merged flow", componentIdentifier);
            return;
        }

        final String value = (String) localDifference.getValueB();
        if (supportedType == DifferenceType.SCHEDULING_STRATEGY_CHANGED) {
            processor.setSchedulingStrategy(value);
        } else {
            processor.setSchedulingPeriod(value);
        }
    }
}
