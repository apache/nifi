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

import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;

import java.io.IOException;

/**
 * Client for interacting with NiFi's Controller Resource.
 */
public interface ControllerClient {

    RegistryClientsEntity getRegistryClients() throws NiFiClientException, IOException;

    RegistryClientEntity getRegistryClient(String id) throws NiFiClientException, IOException;

    RegistryClientEntity createRegistryClient(RegistryClientEntity registryClientEntity) throws NiFiClientException, IOException;

    RegistryClientEntity updateRegistryClient(RegistryClientEntity registryClientEntity) throws NiFiClientException, IOException;

    NodeEntity connectNode(String nodeId, NodeEntity nodeEntity) throws NiFiClientException, IOException;

    NodeEntity deleteNode(String nodeId) throws NiFiClientException, IOException;

    NodeEntity disconnectNode(String nodeId, NodeEntity nodeEntity) throws NiFiClientException, IOException;

    NodeEntity getNode(String nodeId) throws NiFiClientException, IOException;

    ClusterEntity getNodes() throws NiFiClientException, IOException;

    NodeEntity offloadNode(String nodeId, NodeEntity nodeEntity) throws NiFiClientException, IOException;

}
