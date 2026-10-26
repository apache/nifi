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

import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public class ScheduledStateChangedRebaseHandler implements RebaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledStateChangedRebaseHandler.class);

    @Override
    public DifferenceType getSupportedType() {
        return DifferenceType.SCHEDULED_STATE_CHANGED;
    }

    @Override
    public RebaseAnalysis.ClassifiedDifference classify(final FlowDifference localDifference, final Set<FlowDifference> upstreamDifferences,
                                                        final VersionedProcessGroup targetSnapshot) {
        final VersionedComponent localComponent = localDifference.getComponentB();
        if (getSetter(localComponent).isEmpty()) {
            return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE,
                    "Scheduled state change is not supported for component %s of type %s".formatted(localComponent.getIdentifier(), localComponent.getComponentType()));
        }
        if (RebaseHandlerUtils.findConflictingUpstreamDifference(localDifference, upstreamDifferences).isPresent()) {
            return RebaseAnalysis.ClassifiedDifference.conflicting(localDifference, RebaseConflictCode.SAME_SCHEDULED_STATE,
                    "Both local and upstream flows modified the scheduled state of component " + localComponent.getIdentifier());
        }
        return RebaseAnalysis.ClassifiedDifference.compatible(localDifference);
    }

    @Override
    public void apply(final FlowDifference localDifference, final VersionedProcessGroup mergedFlow) {
        final String componentIdentifier = localDifference.getComponentB().getIdentifier();
        final VersionedComponent component = RebaseHandlerUtils.findComponentById(mergedFlow, componentIdentifier);
        if (component == null) {
            logger.warn("Unable to apply scheduled state change: component [{}] not found in merged flow", componentIdentifier);
            return;
        }
        getSetter(component).ifPresent(setter -> setter.accept((ScheduledState) localDifference.getValueB()));
    }

    private Optional<Consumer<ScheduledState>> getSetter(final VersionedComponent component) {
        return switch (component) {
            case VersionedProcessor processor -> Optional.of(processor::setScheduledState);
            case VersionedPort port -> Optional.of(port::setScheduledState);
            case VersionedRemoteGroupPort remoteGroupPort -> Optional.of(remoteGroupPort::setScheduledState);
            case VersionedProcessGroup processGroup -> Optional.of(processGroup::setScheduledState);
            default -> Optional.empty();
        };
    }
}
