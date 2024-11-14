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

import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleRunStatusEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRulesEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientsEntity;
import org.apache.nifi.web.api.entity.NarDetailsEntity;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportRequestEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportResponseEntity;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Client for interacting with NiFi's Controller Resource.
 */
public interface ControllerClient {

    FlowRegistryClientsEntity getRegistryClients() throws NiFiClientException, IOException;

    FlowRegistryClientEntity getRegistryClient(String id) throws NiFiClientException, IOException;

    FlowRegistryClientEntity createRegistryClient(FlowRegistryClientEntity flowRegistryClientEntity) throws NiFiClientException, IOException;

    FlowRegistryClientEntity updateRegistryClient(FlowRegistryClientEntity flowRegistryClientEntity) throws NiFiClientException, IOException;

    NodeEntity connectNode(String nodeId, NodeEntity nodeEntity) throws NiFiClientException, IOException;

    NodeEntity deleteNode(String nodeId) throws NiFiClientException, IOException;

    NodeEntity disconnectNode(String nodeId, NodeEntity nodeEntity) throws NiFiClientException, IOException;

    NodeEntity getNode(String nodeId) throws NiFiClientException, IOException;

    ClusterEntity getNodes() throws NiFiClientException, IOException;

    NodeEntity offloadNode(String nodeId, NodeEntity nodeEntity) throws NiFiClientException, IOException;

    ControllerServiceEntity createControllerService(ControllerServiceEntity controllerService) throws NiFiClientException, IOException;

    ReportingTaskEntity createReportingTask(ReportingTaskEntity reportingTask) throws NiFiClientException, IOException;

    VersionedReportingTaskImportResponseEntity importReportingTasks(VersionedReportingTaskImportRequestEntity importRequestEntity)
            throws NiFiClientException, IOException;

    FlowAnalysisRulesEntity getFlowAnalysisRules() throws NiFiClientException, IOException;

    FlowAnalysisRuleEntity getFlowAnalysisRule(final String id) throws NiFiClientException, IOException;

    PropertyDescriptorEntity getFlowAnalysisRulePropertyDescriptor(final String componentId, final String propertyName, final Boolean sensitive) throws NiFiClientException, IOException;

    FlowAnalysisRuleEntity createFlowAnalysisRule(FlowAnalysisRuleEntity reportingTask) throws NiFiClientException, IOException;

    FlowAnalysisRuleEntity updateFlowAnalysisRule(final FlowAnalysisRuleEntity flowAnalysisRuleEntity) throws NiFiClientException, IOException;

    FlowAnalysisRuleEntity activateFlowAnalysisRule(final String id, final FlowAnalysisRuleRunStatusEntity runStatusEntity) throws NiFiClientException, IOException;

    FlowAnalysisRuleEntity deleteFlowAnalysisRule(final FlowAnalysisRuleEntity flowAnalysisRule) throws NiFiClientException, IOException;

    VerifyConfigRequestEntity submitFlowAnalysisRuleConfigVerificationRequest(final VerifyConfigRequestEntity configRequestEntity) throws NiFiClientException, IOException;

    VerifyConfigRequestEntity getFlowAnalysisRuleConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException;

    VerifyConfigRequestEntity deleteFlowAnalysisRuleConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException;

    ParameterProviderEntity createParamProvider(ParameterProviderEntity paramProvider) throws NiFiClientException, IOException;

    ControllerConfigurationEntity getControllerConfiguration() throws NiFiClientException, IOException;

    ControllerConfigurationEntity updateControllerConfiguration(ControllerConfigurationEntity controllerConfiguration) throws NiFiClientException, IOException;

    NarSummaryEntity uploadNar(String filename, InputStream narContentStream) throws NiFiClientException, IOException;

    NarSummariesEntity getNarSummaries() throws NiFiClientException, IOException;

    NarSummaryEntity getNarSummary(String identifier) throws NiFiClientException, IOException;

    NarSummaryEntity deleteNar(String identifier, boolean forceDelete) throws NiFiClientException, IOException;

    NarDetailsEntity getNarDetails(String identifier) throws NiFiClientException, IOException;

    File downloadNar(String identifier, File outputDir) throws NiFiClientException, IOException;

}
