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
package org.apache.nifi.toolkit.client;

import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.StartVersionControlRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;

import java.io.IOException;

public interface VersionsClient {

    VersionControlInformationEntity getVersionControlInfo(String processGroupId) throws IOException, NiFiClientException;

    VersionedFlowUpdateRequestEntity updateVersionControlInfo(String processGroupId, VersionControlInformationEntity entity)
            throws IOException, NiFiClientException;

    VersionedFlowUpdateRequestEntity getUpdateRequest(String updateRequestId) throws IOException, NiFiClientException;

    VersionedFlowUpdateRequestEntity deleteUpdateRequest(String updateRequestId) throws IOException, NiFiClientException;

    VersionControlInformationEntity startVersionControl(String processGroupId, StartVersionControlRequestEntity startVersionControlRequestEntity) throws IOException, NiFiClientException;

    VersionControlInformationEntity stopVersionControl(ProcessGroupEntity processGroupEntity) throws IOException, NiFiClientException;

    VersionedFlowUpdateRequestEntity initiateRevertFlowVersion(String processGroupId, VersionControlInformationEntity versionControlInformation) throws IOException, NiFiClientException;

    VersionedFlowUpdateRequestEntity getRevertFlowVersionRequest(String requestId) throws IOException, NiFiClientException;

    VersionedFlowUpdateRequestEntity deleteRevertFlowVersionRequest(String requestId) throws IOException, NiFiClientException;

}
