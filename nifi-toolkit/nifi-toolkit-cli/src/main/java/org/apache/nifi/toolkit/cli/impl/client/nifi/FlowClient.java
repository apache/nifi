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
package org.apache.nifi.toolkit.cli.impl.client.nifi;

import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;

import java.io.IOException;

/**
 * Client for FlowResource.
 */
public interface FlowClient {

    /**
     * @return the id of the root process group
     */
    String getRootGroupId() throws NiFiClientException, IOException;

    /**
     * Retrieves the process group with the given id.
     *
     * Passing in "root" as the id will retrieve the root group.
     *
     * @param id the id of the process group to retrieve
     * @return the process group entity
     */
    ProcessGroupFlowEntity getProcessGroup(String id) throws NiFiClientException, IOException;

    /**
     * Schedules the components of a process group.
     *
     * @param processGroupId the id of a process group
     * @param scheduleComponentsEntity the scheduled state to update to
     * @return the entity representing the scheduled state
     */
    ScheduleComponentsEntity scheduleProcessGroupComponents(
            String processGroupId, ScheduleComponentsEntity scheduleComponentsEntity) throws NiFiClientException, IOException;

    /**
     * @return the entity representing the current user accessing the NiFi instance
     */
    CurrentUserEntity getCurrentUser() throws NiFiClientException, IOException;

}
