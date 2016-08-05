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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

public interface ComponentEntityStatusMerger<StatusDtoType> {

    /**
     * Merges status into clientStatus based on the given permissions.
     *
     * @param clientStatus                   The status that will be returned to the client after merging
     * @param clientStatusReadablePermission The read permission of the status that will be returned to the client after merging
     * @param status                         The status to be merged into the client status
     * @param statusReadablePermission       The read permission of the status to be merged into the client status
     * @param statusNodeIdentifier           The {@link NodeIdentifier} of the node from which status was received
     */
    void mergeStatus(final StatusDtoType clientStatus, final boolean clientStatusReadablePermission, final StatusDtoType status,
                     final boolean statusReadablePermission, final NodeIdentifier statusNodeIdentifier);

}
