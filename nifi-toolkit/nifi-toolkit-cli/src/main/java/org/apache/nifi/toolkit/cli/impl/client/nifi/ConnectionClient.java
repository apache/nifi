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

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;

import java.io.IOException;
import java.io.InputStream;

public interface ConnectionClient {
    ConnectionEntity getConnection(String id) throws NiFiClientException, IOException;

    ConnectionEntity deleteConnection(ConnectionEntity connectionEntity) throws NiFiClientException, IOException;

    ConnectionEntity deleteConnection(String id, String clientId, long verison) throws NiFiClientException, IOException;

    ConnectionEntity createConnection(String parentGroupId, ConnectionEntity connectionEntity) throws NiFiClientException, IOException;

    ConnectionEntity updateConnection(ConnectionEntity connectionEntity) throws NiFiClientException, IOException;

    DropRequestEntity emptyQueue(String connectionId) throws NiFiClientException, IOException;

    DropRequestEntity getDropRequest(String connectionId, String dropRequestId) throws NiFiClientException, IOException;

    DropRequestEntity deleteDropRequest(String connectionId, String dropRequestId) throws NiFiClientException, IOException;

    ListingRequestEntity listQueue(String connectionId) throws NiFiClientException, IOException;

    ListingRequestEntity getListingRequest(String connectionId, String listingRequestId) throws NiFiClientException, IOException;

    ListingRequestEntity deleteListingRequest(String connectionId, String listingRequestId) throws NiFiClientException, IOException;

    FlowFileEntity getFlowFile(String connectionId, String flowFileUuid) throws NiFiClientException, IOException;

    FlowFileEntity getFlowFile(String connectionId, String flowFileUuid, String nodeId) throws NiFiClientException, IOException;

    InputStream getFlowFileContent(String connectionId, String flowFileUuid, String nodeId) throws NiFiClientException, IOException;
}
