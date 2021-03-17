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
package org.apache.nifi.web.dao;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface FlowAnalysisRuleDAO {

    /**
     * Determines if the specified flow analysis rule exists.
     *
     * @param flowAnalysisRuleId id
     * @return true if flow analysis rule exists
     */
    boolean hasFlowAnalysisRule(String flowAnalysisRuleId);

    /**
     * Determines whether this flow analysis rule can be create.
     *
     * @param flowAnalysisRuleDTO dto
     */
    void verifyCreate(FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Creates a flow analysis rule.
     *
     * @param flowAnalysisRuleDTO The flow analysis rule DTO
     * @return The flow analysis rule
     */
    FlowAnalysisRuleNode createFlowAnalysisRule(FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Gets the specified flow analysis rule.
     *
     * @param flowAnalysisRuleId The flow analysis rule id
     * @return The flow analysis rule
     */
    FlowAnalysisRuleNode getFlowAnalysisRule(String flowAnalysisRuleId);

    /**
     * Gets all the flow analysis rules.
     *
     * @return The flow analysis rules
     */
    Set<FlowAnalysisRuleNode> getFlowAnalysisRules();

    /**
     * Updates the specified flow analysis rule.
     *
     * @param flowAnalysisRuleDTO The flow analysis rule DTO
     * @return The flow analysis rule
     */
    FlowAnalysisRuleNode updateFlowAnalysisRule(FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Determines whether this flow analysis rule can be updated.
     *
     * @param flowAnalysisRuleDTO dto
     */
    void verifyUpdate(FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Verifies the Flow Analysis Rule is in a state in which its configuration can be verified
     * @param flowAnalysisRuleId the id of the Flow Analysis Rule
     */
    void verifyConfigVerification(String flowAnalysisRuleId);
    /**
     * Performs verification of the Configuration for the Flow Analysis Rule with the given ID
     * @param flowAnalysisRuleId the id of the Flow Analysis Rule
     * @param properties the configured properties to verify
     * @return verification results
     */
    List<ConfigVerificationResultDTO> verifyConfiguration(String flowAnalysisRuleId, Map<String, String> properties);

    /**
     * Determines whether this flow analysis rule can be removed.
     *
     * @param flowAnalysisRuleId id
     */
    void verifyDelete(String flowAnalysisRuleId);

    /**
     * Deletes the specified flow analysis rule.
     *
     * @param flowAnalysisRuleId The flow analysis rule id
     */
    void deleteFlowAnalysisRule(String flowAnalysisRuleId);

    /**
     * Gets the specified flow analysis rule.
     *
     * @param flowAnalysisRuleId flow analysis rule id
     * @return state map
     */
    StateMap getState(String flowAnalysisRuleId, Scope scope);

    /**
     * Verifies the flow analysis rule can clear state.
     *
     * @param flowAnalysisRuleId flow analysis rule id
     */
    void verifyClearState(String flowAnalysisRuleId);

    /**
     * Clears the state of the specified flow analysis rule.
     *
     * @param flowAnalysisRuleId flow analysis rule id
     */
    void clearState(String flowAnalysisRuleId);
}
