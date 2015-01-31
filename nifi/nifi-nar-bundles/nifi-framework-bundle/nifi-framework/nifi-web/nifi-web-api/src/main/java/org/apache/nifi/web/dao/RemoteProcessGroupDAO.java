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
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

/**
 *
 */
public interface RemoteProcessGroupDAO {

    /**
     * Determines if the specified remote process group exists.
     *
     * @param groupId
     * @param remoteProcessGroupId
     * @return
     */
    boolean hasRemoteProcessGroup(String groupId, String remoteProcessGroupId);

    /**
     * Creates a remote process group reference.
     *
     * @param groupId
     * @param remoteProcessGroup The remote process group
     * @return The remote process group
     */
    RemoteProcessGroup createRemoteProcessGroup(String groupId, RemoteProcessGroupDTO remoteProcessGroup);

    /**
     * Gets the specified remote process group.
     *
     * @param groupId
     * @param remoteProcessGroupId The remote process group id
     * @return The remote process group
     */
    RemoteProcessGroup getRemoteProcessGroup(String groupId, String remoteProcessGroupId);

    /**
     * Gets all of the remote process groups.
     *
     * @param groupId
     * @return The remote process groups
     */
    Set<RemoteProcessGroup> getRemoteProcessGroups(String groupId);

    /**
     * Verifies the specified remote process group can be updated.
     *
     * @param groupId
     * @param remoteProcessGroup
     */
    void verifyUpdate(String groupId, RemoteProcessGroupDTO remoteProcessGroup);

    /**
     * Verifies the specified remote process group input port can be updated.
     *
     * @param groupId
     * @param remoteProcessGroupId
     * @param remoteProcessGroupPort
     */
    void verifyUpdateInputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPort);

    /**
     * Verifies the specified remote process group input port can be updated.
     *
     * @param groupId
     * @param remoteProcessGroupId
     * @param remoteProcessGroupPort
     */
    void verifyUpdateOutputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPort);

    /**
     * Updates the specified remote process group.
     *
     * @param groupId
     * @param remoteProcessGroup The remote process group
     * @return The remote process group
     */
    RemoteProcessGroup updateRemoteProcessGroup(String groupId, RemoteProcessGroupDTO remoteProcessGroup);

    /**
     * Updates the specified remote process group input port.
     *
     * @param groupId
     * @param remoteProcessGroupId
     * @param remoteProcessGroupPort
     * @return
     */
    RemoteGroupPort updateRemoteProcessGroupInputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPort);

    /**
     * Updates the specified remote process group output port.
     *
     * @param groupId
     * @param remoteProcessGroupId
     * @param remoteProcessGroupPort
     * @return
     */
    RemoteGroupPort updateRemoteProcessGroupOutputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPort);

    /**
     * Verifies the specified remote process group can be removed.
     *
     * @param groupId
     * @param remoteProcessGroupId
     */
    void verifyDelete(String groupId, String remoteProcessGroupId);

    /**
     * Deletes the specified remote process group.
     *
     * @param groupId
     * @param remoteProcessGroupId The remote process group id
     */
    void deleteRemoteProcessGroup(String groupId, String remoteProcessGroupId);
}
