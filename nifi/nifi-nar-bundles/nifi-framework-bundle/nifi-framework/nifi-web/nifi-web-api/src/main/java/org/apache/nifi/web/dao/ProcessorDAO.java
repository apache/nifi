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

import java.util.Set;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.web.api.dto.ProcessorDTO;

/**
 *
 */
public interface ProcessorDAO {

    /**
     * Determines if the specified processor is loaded.
     *
     * @param groupId
     * @param id
     * @return
     */
    boolean hasProcessor(String groupId, String id);

    /**
     * Creates a new Processor.
     *
     * @param groupId
     * @param processorDTO The processor DTO
     * @return The new Processor
     */
    ProcessorNode createProcessor(String groupId, ProcessorDTO processorDTO);

    /**
     * Gets the Processor transfer object for the specified id.
     *
     * @param groupId
     * @param id Id of the processor to return
     * @return The Processor
     */
    ProcessorNode getProcessor(String groupId, String id);

    /**
     * Gets all the Processor transfer objects for this controller.
     *
     * @param groupId
     * @return List of all the Processors
     */
    Set<ProcessorNode> getProcessors(String groupId);

    /**
     * Verifies the specified processor can be updated.
     *
     * @param groupId
     * @param processorDTO
     */
    void verifyUpdate(String groupId, ProcessorDTO processorDTO);

    /**
     * Updates the configuration for the processor using the specified
     * processorDTO.
     *
     * @param groupId
     * @param processorDTO
     * @return 
     */
    ProcessorNode updateProcessor(String groupId, ProcessorDTO processorDTO);

    /**
     * Verifies the specified processor can be removed.
     *
     * @param groupId
     * @param processorId
     */
    void verifyDelete(String groupId, String processorId);

    /**
     * Deletes the specified processor.
     *
     * @param groupId
     * @param processorId The processor id to delete
     */
    void deleteProcessor(String groupId, String processorId);

}
