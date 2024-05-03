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
package org.apache.nifi.web.api.entity;

import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleViolationDTO;

import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API.
 *  This particular entity holds a reference to a collection of {@link FlowAnalysisRuleDTO} and another collection of {@link FlowAnalysisRuleViolationDTO}.
 */
@XmlRootElement(name = "flowAnalysisResultEntity")
public class FlowAnalysisResultEntity extends Entity {
    public FlowAnalysisResultEntity() {
    }

    private boolean flowAnalysisPending;

    private List<FlowAnalysisRuleDTO> rules = new ArrayList<>();
    private List<FlowAnalysisRuleViolationDTO> ruleViolations = new ArrayList<>();

    /**
     * @return true if a flow analysis is going to be scheduled due to flow changes, false otherwise
     */
    public boolean isFlowAnalysisPending() {
        return flowAnalysisPending;
    }

    public void setFlowAnalysisPending(boolean flowAnalysisPending) {
        this.flowAnalysisPending = flowAnalysisPending;
    }

    /**
     * @return set of flow analysis rules that are being serialized
     */
    public List<FlowAnalysisRuleDTO> getRules() {
        return rules;
    }

    public void setRules(List<FlowAnalysisRuleDTO> rules) {
        this.rules = rules;
    }

    /**
     * @return set of flow analysis results that are being serialized
     */
    public List<FlowAnalysisRuleViolationDTO> getRuleViolations() {
        return ruleViolations;
    }

    public void setRuleViolations(List<FlowAnalysisRuleViolationDTO> ruleViolations) {
        this.ruleViolations = ruleViolations;
    }
}
