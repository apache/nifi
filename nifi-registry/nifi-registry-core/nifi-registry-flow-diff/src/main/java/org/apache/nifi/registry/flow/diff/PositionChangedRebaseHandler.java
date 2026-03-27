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

import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class PositionChangedRebaseHandler implements RebaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(PositionChangedRebaseHandler.class);

    @Override
    public DifferenceType getSupportedType() {
        return DifferenceType.POSITION_CHANGED;
    }

    @Override
    public RebaseAnalysis.ClassifiedDifference classify(final FlowDifference localDifference, final Set<FlowDifference> upstreamDifferences,
                                                        final VersionedProcessGroup targetSnapshot) {
        return RebaseAnalysis.ClassifiedDifference.compatible(localDifference);
    }

    @Override
    public void apply(final FlowDifference localDifference, final VersionedProcessGroup mergedFlow) {
        final String componentIdentifier = localDifference.getComponentB().getIdentifier();
        final VersionedComponent component = RebaseHandlerUtils.findComponentById(mergedFlow, componentIdentifier);
        if (component == null) {
            logger.warn("Unable to apply position change: component [{}] not found in merged flow", componentIdentifier);
            return;
        }
        component.setPosition((Position) localDifference.getValueB());
    }
}
