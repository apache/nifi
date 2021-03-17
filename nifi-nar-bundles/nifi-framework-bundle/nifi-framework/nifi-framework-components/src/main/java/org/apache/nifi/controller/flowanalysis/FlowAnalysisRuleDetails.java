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
package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;

/**
 * Holder for StandardFlowAnalysisRuleNode to atomically swap out the component.
 */
class FlowAnalysisRuleDetails {

    private final FlowAnalysisRule flowAnalysisRule;
    private final TerminationAwareLogger componentLog;
    private final BundleCoordinate bundleCoordinate;

    public FlowAnalysisRuleDetails(final LoggableComponent<FlowAnalysisRule> flowAnalysisRule) {
        this.flowAnalysisRule = flowAnalysisRule.getComponent();
        this.componentLog = flowAnalysisRule.getLogger();
        this.bundleCoordinate = flowAnalysisRule.getBundleCoordinate();
    }

    public FlowAnalysisRule getFlowAnalysisRule() {
        return flowAnalysisRule;
    }

    public TerminationAwareLogger getComponentLog() {
        return componentLog;
    }

    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }
}
