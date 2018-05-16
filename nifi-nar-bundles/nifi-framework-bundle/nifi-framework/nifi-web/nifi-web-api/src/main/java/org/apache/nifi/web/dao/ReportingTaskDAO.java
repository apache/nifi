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
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;

import java.util.Set;

public interface ReportingTaskDAO {

    /**
     * Determines if the specified reporting task exists.
     *
     * @param reportingTaskId id
     * @return true if reporting task exists
     */
    boolean hasReportingTask(String reportingTaskId);

    /**
     * Determines whether this reporting task can be create.
     *
     * @param reportingTaskDTO dto
     */
    void verifyCreate(ReportingTaskDTO reportingTaskDTO);

    /**
     * Creates a reporting task.
     *
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task
     */
    ReportingTaskNode createReportingTask(ReportingTaskDTO reportingTaskDTO);

    /**
     * Gets the specified reporting task.
     *
     * @param reportingTaskId The reporting task id
     * @return The reporting task
     */
    ReportingTaskNode getReportingTask(String reportingTaskId);

    /**
     * Gets all of the reporting tasks.
     *
     * @return The reporting tasks
     */
    Set<ReportingTaskNode> getReportingTasks();

    /**
     * Updates the specified reporting task.
     *
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task
     */
    ReportingTaskNode updateReportingTask(ReportingTaskDTO reportingTaskDTO);

    /**
     * Determines whether this reporting task can be updated.
     *
     * @param reportingTaskDTO dto
     */
    void verifyUpdate(ReportingTaskDTO reportingTaskDTO);

    /**
     * Determines whether this reporting task can be removed.
     *
     * @param reportingTaskId id
     */
    void verifyDelete(String reportingTaskId);

    /**
     * Deletes the specified reporting task.
     *
     * @param reportingTaskId The reporting task id
     */
    void deleteReportingTask(String reportingTaskId);

    /**
     * Gets the specified reporting task.
     *
     * @param reportingTaskId reporting task id
     * @return state map
     */
    StateMap getState(String reportingTaskId, Scope scope);

    /**
     * Verifies the reporting task can clear state.
     *
     * @param reportingTaskId reporting task id
     */
    void verifyClearState(String reportingTaskId);

    /**
     * Clears the state of the specified reporting task.
     *
     * @param reportingTaskId reporting task id
     */
    void clearState(String reportingTaskId);
}
