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

import org.apache.nifi.registry.flow.FlowRegistryBranch;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.FlowRegistryClientUserContext;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;

import java.util.Set;

public interface FlowRegistryDAO {

    FlowRegistryClientNode createFlowRegistryClient(FlowRegistryClientDTO flowRegistryClientDto);

    FlowRegistryClientNode updateFlowRegistryClient(FlowRegistryClientDTO flowRegistryClientDto);

    FlowRegistryClientNode getFlowRegistryClient(String registryId);

    Set<FlowRegistryClientNode> getFlowRegistryClients();

    Set<FlowRegistryClientNode> getFlowRegistryClientsForUser(FlowRegistryClientUserContext context);

    Set<FlowRegistryBranch> getBranchesForUser(FlowRegistryClientUserContext context, String registryId);

    FlowRegistryBranch getDefaultBranchForUser(FlowRegistryClientUserContext context, String registryId);

    Set<FlowRegistryBucket> getBucketsForUser(FlowRegistryClientUserContext context, String registryId, String branch);

    Set<RegisteredFlow> getFlowsForUser(FlowRegistryClientUserContext context, String registryId, String branch, String bucketId);

    RegisteredFlow getFlowForUser(FlowRegistryClientUserContext context, String registryId, String branch, String bucketId, String flowId);

    Set<RegisteredFlowSnapshotMetadata> getFlowVersionsForUser(FlowRegistryClientUserContext context, String branch, String registryId, String bucketId, String flowId);

    FlowRegistryClientNode removeFlowRegistry(String registryId);

}
