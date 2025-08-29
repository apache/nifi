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
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.api.dto.ComponentStateDTO;

public interface ComponentStateDAO {

    /**
     * Gets the state map for the specified processor.
     *
     * @param processor processor
     * @param scope     scope
     * @return state map
     */
    StateMap getState(ProcessorNode processor, Scope scope);

    /**
     * Clears the state for the specified processor.
     *
     * @param processor processor
     */
    default void clearState(ProcessorNode processor) {
        clearState(processor, null);
    }

    /**
     * Clears the state for the specified processor.
     *
     * @param processor         processor
     * @param componentStateDTO component state DTO
     */
    void clearState(ProcessorNode processor, final ComponentStateDTO componentStateDTO);

    /**
     * Gets the state map for the specified controller service.
     *
     * @param controllerService controller service
     * @param scope     scope
     * @return state map
     */
    StateMap getState(ControllerServiceNode controllerService, Scope scope);

    /**
     * Clears the state for the specified controller service.
     *
     * @param controllerService controller service
     * @param componentStateDTO component state DTO
     */
    void clearState(ControllerServiceNode controllerService, final ComponentStateDTO componentStateDTO);

    /**
     * Gets the state for the specified reporting task.
     *
     * @param reportingTask reporting task
     * @param scope     scope
     * @return state map
     */
    StateMap getState(ReportingTaskNode reportingTask, Scope scope);

    /**
     * Clears the state for the specified reporting task.
     *
     * @param reportingTask     reporting task
     * @param componentStateDTO component state DTO
     */
    void clearState(ReportingTaskNode reportingTask, final ComponentStateDTO componentStateDTO);

    /**
     * Gets the state for the specified flow analysis rule.
     *
     * @param flowAnalysisRule flow analysis rule
     * @param scope     scope
     * @return state map
     */

    StateMap getState(FlowAnalysisRuleNode flowAnalysisRule, Scope scope);

    /**
     * Clears the state for the specified flow analysis rule.
     *
     * @param flowAnalysisRule  flow analysis rule
     * @param componentStateDTO component state DTO
     */
    void clearState(FlowAnalysisRuleNode flowAnalysisRule, final ComponentStateDTO componentStateDTO);

    /**
     * Gets the state for the specified parameter provider.
     *
     * @param parameterProvider parameter provider
     * @param scope     scope
     * @return state map
     */
    StateMap getState(ParameterProviderNode parameterProvider, Scope scope);

    /**
     * Clears the state for the specified parameter provider.
     *
     * @param parameterProvider parameter provider
     * @param componentStateDTO component state DTO
     */
    void clearState(ParameterProviderNode parameterProvider, final ComponentStateDTO componentStateDTO);

    /**
     * Gets the state map for the specified RemoteProcessGroup.
     *
     * @param remoteProcessGroup RemoteProcessGroup
     * @param scope     scope
     * @return state map
     */
    StateMap getState(RemoteProcessGroup remoteProcessGroup, Scope scope);
}
