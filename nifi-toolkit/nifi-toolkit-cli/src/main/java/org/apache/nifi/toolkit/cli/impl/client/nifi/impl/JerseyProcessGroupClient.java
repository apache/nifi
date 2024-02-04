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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.toolkit.cli.impl.util.FileUtils;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.CopySnippetRequestEntity;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;

import java.io.File;
import java.io.IOException;

/**
* Jersey implementation of ProcessGroupClient.
*/
public class JerseyProcessGroupClient extends AbstractJerseyClient implements ProcessGroupClient {

   private final WebTarget processGroupsTarget;

   public JerseyProcessGroupClient(final WebTarget baseTarget) {
       this(baseTarget, null);
   }

   public JerseyProcessGroupClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
       super(requestConfig);
       this.processGroupsTarget = baseTarget.path("/process-groups");
   }

   @Override
   public ProcessGroupEntity createProcessGroup(final String parentGroupdId, final ProcessGroupEntity entity)
           throws NiFiClientException, IOException {
       return createProcessGroup(parentGroupdId, entity, true);
   }

   @Override
   public ProcessGroupEntity createProcessGroup(final String parentGroupdId, final ProcessGroupEntity entity, boolean keepExisting)
           throws NiFiClientException, IOException {

       if (StringUtils.isBlank(parentGroupdId)) {
           throw new IllegalArgumentException("Parent process group id cannot be null or blank");
       }

       if (entity == null){
           throw new IllegalArgumentException("Process group entity cannot be null");
       }

       return executeAction("Error creating process group", () -> {
           final WebTarget target = processGroupsTarget
                   .path("{id}/process-groups")
                   .queryParam("parameterContextHandlingStrategy", keepExisting ? "KEEP_EXISTING" : "REPLACE")
                   .resolveTemplate("id", parentGroupdId);

           return getRequestBuilder(target).post(
                   Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                   ProcessGroupEntity.class
           );
       });
   }

   @Override
   public ProcessGroupEntity getProcessGroup(final String processGroupId) throws NiFiClientException, IOException {
       if (StringUtils.isBlank(processGroupId)) {
           throw new IllegalArgumentException("Process group id cannot be null or blank");
       }

       return executeAction("Error getting process group", () -> {
           final WebTarget target = processGroupsTarget
                   .path("{id}")
                   .resolveTemplate("id", processGroupId);

           return getRequestBuilder(target).get(ProcessGroupEntity.class);
       });
   }

   @Override
   public ProcessGroupEntity updateProcessGroup(final ProcessGroupEntity entity)
           throws NiFiClientException, IOException {

       if (entity == null){
           throw new IllegalArgumentException("Process group entity cannot be null");
       }

       return executeAction("Error updating process group", () -> {
           final WebTarget target = processGroupsTarget
                   .path("{id}")
                   .resolveTemplate("id", entity.getId());

           return getRequestBuilder(target).put(
                   Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                   ProcessGroupEntity.class
           );
       });
   }

   @Override
   public ControllerServiceEntity  createControllerService(
           final String processGroupId, final ControllerServiceEntity controllerService) throws NiFiClientException, IOException {
       if (StringUtils.isBlank(processGroupId)) {
           throw new IllegalArgumentException("Process group id cannot be null or blank");
       }

       if (controllerService == null) {
           throw new IllegalArgumentException("Controller service entity cannot be null");
       }

       return executeAction("Error creating controller service", () -> {
           final WebTarget target = processGroupsTarget
                   .path("{id}/controller-services")
                   .resolveTemplate("id", processGroupId);

           return getRequestBuilder(target).post(
                   Entity.entity(controllerService, MediaType.APPLICATION_JSON),
                   ControllerServiceEntity.class
           );
       });
   }

   @Override
   public ProcessGroupReplaceRequestEntity replaceProcessGroup(final String processGroupId, final ProcessGroupImportEntity importEntity)
           throws NiFiClientException, IOException {

       if (StringUtils.isBlank(processGroupId)) {
           throw new IllegalArgumentException("Process group id cannot be null or blank");
       }

       if (importEntity == null || importEntity.getVersionedFlowSnapshot() == null) {
           throw new IllegalArgumentException("ProcessGroupImportEntity cannot be null and must have a non-null VersionedFlowSnapshot");
       }

       return executeAction("Error creating process group replacement request", () -> {
           final WebTarget target = processGroupsTarget
                   .path("{processGroupId}/replace-requests")
                   .resolveTemplate("processGroupId", processGroupId);

           return getRequestBuilder(target).post(
                   Entity.entity(importEntity, MediaType.APPLICATION_JSON_TYPE),
                   ProcessGroupReplaceRequestEntity.class
           );
       });
   }

   @Override
   public ProcessGroupReplaceRequestEntity getProcessGroupReplaceRequest(final String processGroupId, final String requestId)
           throws NiFiClientException, IOException {

       if (StringUtils.isBlank(processGroupId)) {
           throw new IllegalArgumentException("Process group id cannot be null or blank");
       }

       if (StringUtils.isBlank(requestId)) {
           throw new IllegalArgumentException("Request id cannot be null or blank");
       }

       return executeAction("Error getting process group replacement request", () -> {
           final WebTarget target = processGroupsTarget
                   .path("replace-requests/{requestId}")
                   .resolveTemplate("requestId", requestId);

           return getRequestBuilder(target).get(ProcessGroupReplaceRequestEntity.class);
       });
   }

   @Override
   public ProcessGroupReplaceRequestEntity deleteProcessGroupReplaceRequest(final String processGroupId, final String requestId)
           throws NiFiClientException, IOException {

       if (StringUtils.isBlank(processGroupId)) {
           throw new IllegalArgumentException("Process group id cannot be null or blank");
       }

       if (StringUtils.isBlank(requestId)) {
           throw new IllegalArgumentException("Request id cannot be null or blank");
       }

       return executeAction("Error deleting process group replacement request", () -> {
           final WebTarget target = processGroupsTarget
                   .path("replace-requests/{requestId}")
                   .resolveTemplate("requestId", requestId);

           return getRequestBuilder(target).delete(ProcessGroupReplaceRequestEntity.class);
       });
   }

   @Override
   public FlowEntity copySnippet(final String processGroupId, final CopySnippetRequestEntity copySnippetRequestEntity) throws NiFiClientException, IOException {
       if (StringUtils.isBlank(processGroupId)) {
           throw new IllegalArgumentException("Process group id cannot be null or blank");
       }

       if (copySnippetRequestEntity == null) {
           throw new IllegalArgumentException("Snippet Request Entity cannot be null");
       }

       return executeAction("Error copying snippet to Process Group", () -> {
           final WebTarget target = processGroupsTarget
               .path("{id}/snippet-instance")
               .resolveTemplate("id", processGroupId);

           return getRequestBuilder(target).post(
               Entity.entity(copySnippetRequestEntity, MediaType.APPLICATION_JSON_TYPE),
               FlowEntity.class);
       });
   }

   @Override
   public FlowComparisonEntity getLocalModifications(final String processGroupId) throws NiFiClientException, IOException {
       if (StringUtils.isBlank(processGroupId)) {
           throw new IllegalArgumentException("Process group id cannot be null or blank");
       }

       return executeAction("Error retrieving list of local flow modifications", () -> {
           final WebTarget target = processGroupsTarget
               .path("{id}/local-modifications")
               .resolveTemplate("id", processGroupId);

           return getRequestBuilder(target).get(FlowComparisonEntity.class);
       });
   }

    @Override
    public File exportProcessGroup(final String processGroupId, final boolean includeReferencedServices, final File outputFile) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error getting process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/download")
                    .resolveTemplate("id", processGroupId)
                    .queryParam("includeReferencedServices", includeReferencedServices);

            final Response response = getRequestBuilder(target)
                    .accept(MediaType.APPLICATION_JSON)
                    .get();
            return FileUtils.getFileContent(response, outputFile);
        });
    }

}