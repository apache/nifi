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

package org.apache.nifi.flow;

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.flowanalysis.EnforcementPolicy;

public class VersionedFlowAnalysisRule extends VersionedConfigurableExtension {

    private ScheduledState scheduledState;
    private EnforcementPolicy enforcementPolicy;

    @ApiModelProperty("How to handle violations.")
    public EnforcementPolicy getEnforcementPolicy() {
        return enforcementPolicy;
    }

    public void setEnforcementPolicy(EnforcementPolicy enforcementPolicy) {
        this.enforcementPolicy = enforcementPolicy;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.FLOW_ANALYSIS_RULE;
    }

    @ApiModelProperty("Indicates the scheduled state for the flow analysis rule")
    public ScheduledState getScheduledState() {
        return scheduledState;
    }

    public void setScheduledState(final ScheduledState scheduledState) {
        this.scheduledState = scheduledState;
    }
}
