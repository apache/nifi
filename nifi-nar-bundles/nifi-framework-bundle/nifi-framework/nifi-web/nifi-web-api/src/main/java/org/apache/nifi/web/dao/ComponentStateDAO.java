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
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;

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
    void clearState(ProcessorNode processor);

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
     */
    void clearState(ControllerServiceNode controllerService);

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
     * @param reportingTask reporting task
     */
    void clearState(ReportingTaskNode reportingTask);
}
