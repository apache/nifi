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

import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VariableRegistryUpdateRequestEntity;

import java.io.IOException;

/**
 * Client for ProcessGroupResource.
 */
public interface ProcessGroupClient {

    ProcessGroupEntity createProcessGroup(String parentGroupdId, ProcessGroupEntity entity)
            throws NiFiClientException, IOException;

    ProcessGroupEntity getProcessGroup(String processGroupId) throws NiFiClientException, IOException;

    ProcessGroupEntity updateProcessGroup(ProcessGroupEntity entity) throws NiFiClientException, IOException;

    VariableRegistryEntity getVariables(String processGroupId) throws NiFiClientException, IOException;

    VariableRegistryUpdateRequestEntity updateVariableRegistry(
            String processGroupId, VariableRegistryEntity variableRegistryEntity)
            throws NiFiClientException, IOException;

    VariableRegistryUpdateRequestEntity getVariableRegistryUpdateRequest(String processGroupdId, String requestId)
            throws NiFiClientException, IOException;

    VariableRegistryUpdateRequestEntity deleteVariableRegistryUpdateRequest(String processGroupdId, String requestId)
            throws NiFiClientException, IOException;

    ControllerServiceEntity createControllerService(String processGroupId, ControllerServiceEntity controllerService)
            throws NiFiClientException, IOException;

    TemplateEntity uploadTemplate(String processGroupId, TemplateDTO templateDTO) throws NiFiClientException, IOException;

    ProcessGroupReplaceRequestEntity replaceProcessGroup(String processGroupId, ProcessGroupImportEntity importEntity)
            throws NiFiClientException, IOException;

    ProcessGroupReplaceRequestEntity getProcessGroupReplaceRequest(String processGroupId, String requestId)
            throws NiFiClientException, IOException;

    ProcessGroupReplaceRequestEntity deleteProcessGroupReplaceRequest(String processGroupId, String requestId)
            throws NiFiClientException, IOException;

}
