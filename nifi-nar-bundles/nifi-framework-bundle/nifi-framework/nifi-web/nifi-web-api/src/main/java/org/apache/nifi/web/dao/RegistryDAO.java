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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.web.api.dto.RegistryDTO;

import java.util.Set;

public interface RegistryDAO {

    FlowRegistry createFlowRegistry(RegistryDTO registryDto);

    FlowRegistry getFlowRegistry(String registryId);

    Set<FlowRegistry> getFlowRegistries();

    Set<FlowRegistry> getFlowRegistriesForUser(NiFiUser user);

    Set<Bucket> getBucketsForUser(String registry, NiFiUser user);

    Set<VersionedFlow> getFlowsForUser(String registryId, String bucketId, NiFiUser user);

    Set<VersionedFlowSnapshotMetadata> getFlowVersionsForUser(String registryId, String bucketId, String flowId, NiFiUser user);

    FlowRegistry removeFlowRegistry(String registryId);

}
