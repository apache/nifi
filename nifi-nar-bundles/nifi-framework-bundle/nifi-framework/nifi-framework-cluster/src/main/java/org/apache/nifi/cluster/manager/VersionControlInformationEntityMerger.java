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

import java.util.Map;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;

public class VersionControlInformationEntityMerger {

    public void merge(final VersionControlInformationEntity clientEntity, final Map<NodeIdentifier, VersionControlInformationEntity> entityMap) {

        final VersionControlInformationDTO clientDto = clientEntity.getVersionControlInformation();

        // We need to merge the 'current' and 'modified' flags because these are updated by nodes in the background. Since
        // the nodes can synchronize with the Flow Registry at different intervals, we have to determine how to handle these
        // flags if different nodes report different values for them.
        entityMap.values().stream()
            .filter(entity -> entity != clientEntity)
            .forEach(entity -> {
                final VersionControlInformationDTO dto = entity.getVersionControlInformation();

                // We consider the flow to be current only if ALL nodes indicate that it is current
                clientDto.setCurrent(Boolean.TRUE.equals(clientDto.getCurrent()) && Boolean.TRUE.equals(dto.getCurrent()));

                // We consider the flow to be modified if ANY node indicates that it is modified
                clientDto.setModified(Boolean.TRUE.equals(clientDto.getModified()) || Boolean.TRUE.equals(dto.getModified()));
            });
    }

}
