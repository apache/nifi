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
package org.apache.nifi.web;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.NodeSystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.ProcessorHistoryDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ClusterConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterPortStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterRemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterStatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;

/**
 * Defines the NiFiServiceFacade interface.
 */
public interface NiFiServiceFacade {

    // ----------------------------------------
    // Controller methods
    // ----------------------------------------
    ControllerDTO getController();

    /**
     * Searches the controller for the specified query string.
     *
     * @param query
     * @return
     */
    SearchResultsDTO searchController(String query);

    /**
     * Submits a provenance request.
     *
     * @param query
     * @return
     */
    ProvenanceDTO submitProvenance(ProvenanceDTO query);

    /**
     * Submits a new replay request.
     *
     * @param eventId
     * @return
     */
    ProvenanceEventDTO submitReplay(Long eventId);

    /**
     * Gets the content for the specified claim.
     *
     * @param eventId
     * @param uri
     * @param contentDirection
     * @return
     */
    DownloadableContent getContent(Long eventId, String uri, ContentDirection contentDirection);

    /**
     * Retrieves provenance.
     *
     * @param queryId
     * @return
     */
    ProvenanceDTO getProvenance(String queryId);

    /**
     * Deletes provenance.
     *
     * @param queryId
     */
    void deleteProvenance(String queryId);

    /**
     * Returns the available options for searching provenance.
     *
     * @return
     */
    ProvenanceOptionsDTO getProvenanceSearchOptions();

    /**
     * Submits a lineage request.
     *
     * @param lineage
     * @return
     */
    LineageDTO submitLineage(LineageDTO lineage);

    /**
     * Gets the lineage with the specified id.
     *
     * @param lineageId
     * @return
     */
    LineageDTO getLineage(String lineageId);

    /**
     * Deletes the lineage with the specified id.
     *
     * @param lineageId
     */
    void deleteLineage(String lineageId);

    /**
     * Gets the provenance event with the specified id.
     *
     * @param id
     * @return
     */
    ProvenanceEventDTO getProvenanceEvent(Long id);

    /**
     * Gets the configuration for this controller.
     *
     * @return Controller configuration transfer object
     */
    ControllerConfigurationDTO getControllerConfiguration();

    /**
     * Updates the configuration for this controller.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerConfigurationDTO Controller configuration DTO
     * @return Controller configuration DTO
     */
    ConfigurationSnapshot<ControllerConfigurationDTO> updateControllerConfiguration(Revision revision, ControllerConfigurationDTO controllerConfigurationDTO);

    /**
     * Creates a new archive of the flow configuration.
     *
     * @param revision Revision to compare with current base revision
     * @return
     */
    ConfigurationSnapshot<Void> createArchive(Revision revision);

    /**
     * Sets the annotation data for a processor.
     *
     * @param revision the revision to compare with current base revision
     * @param processorId the id of the processor to update
     * @param annotationData the annotation data
     *
     * @return
     */
    ConfigurationSnapshot<ProcessorDTO> setProcessorAnnotationData(Revision revision, String processorId, String annotationData);

    /**
     * Returns the process group status.
     *
     * @param groupId
     * @return The process group status
     */
    ProcessGroupStatusDTO getProcessGroupStatus(String groupId);

    /**
     * Gets the process group status history.
     *
     * @param groupId
     * @return
     */
    StatusHistoryDTO getProcessGroupStatusHistory(String groupId);

    /**
     * Returns the controller status.
     *
     * @return
     */
    ControllerStatusDTO getControllerStatus();

    /**
     * Updates the specified counter by setting its value to 0.
     *
     * @param counterId
     * @return
     */
    CounterDTO updateCounter(String counterId);

    /**
     * Returns the counters.
     *
     * @return The counters
     */
    CountersDTO getCounters();

    /**
     * Returns the list of processor types.
     *
     * @return The list of available processor types
     */
    Set<DocumentedTypeDTO> getProcessorTypes();

    /**
     * Returns the list of prioritizer types.
     *
     * @return The list of available prioritizer types
     */
    Set<DocumentedTypeDTO> getWorkQueuePrioritizerTypes();

    /**
     * Returns the current revision.
     *
     * @return
     */
    RevisionDTO getRevision();

    // ----------------------------------------
    // Template methods
    // ----------------------------------------
    /**
     * Creates a new Template based off the specified snippet.
     *
     * @param name
     * @param description
     * @param snippetId
     * @return
     */
    TemplateDTO createTemplate(String name, String description, String snippetId);

    /**
     * Imports the specified Template.
     *
     * @param templateDTO The template dto
     * @return The new template dto
     */
    TemplateDTO importTemplate(TemplateDTO templateDTO);

    /**
     * Instantiate the corresponding template.
     *
     * @param revision
     * @param groupId
     * @param templateId
     * @param originX
     * @param originY
     * @return 
     */
    ConfigurationSnapshot<FlowSnippetDTO> createTemplateInstance(Revision revision, String groupId, Double originX, Double originY, String templateId);

    /**
     * Gets the template with the specified id.
     *
     * @param id
     * @return
     */
    TemplateDTO getTemplate(String id);

    /**
     * Gets the template, includes contents, with the specified id.
     *
     * @param id
     * @return
     */
    TemplateDTO exportTemplate(String id);

    /**
     * Gets all templates.
     *
     * @return
     */
    Set<TemplateDTO> getTemplates();

    /**
     * Deletes the specified template.
     *
     * @param id The id of the template
     */
    void deleteTemplate(String id);

    // ----------------------------------------
    // Processor methods
    // ----------------------------------------
    /**
     * Creates a new Processor.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId Group id
     * @param processorDTO The processor DTO
     * @return The new processor DTO
     */
    ConfigurationSnapshot<ProcessorDTO> createProcessor(Revision revision, String groupId, ProcessorDTO processorDTO);

    /**
     * Gets the Processor transfer object for the specified id.
     *
     * @param groupId Id of the processor group containing the processor
     * @param id Id of the processor to return
     * @return The Processor transfer object
     */
    ProcessorDTO getProcessor(String groupId, String id);

    /**
     * Gets the Processor transfer object for the specified id.
     *
     * @param id Id of the processor to return
     * @return The Processor transfer object
     */
    ProcessorDTO getProcessor(String id);

    /**
     * Gets the processor status history.
     *
     * @param groupId
     * @param id
     * @return
     */
    StatusHistoryDTO getProcessorStatusHistory(String groupId, String id);

    /**
     * Gets all the Processor transfer objects for this controller.
     *
     * @param groupId
     * @return List of all the Processor transfer object
     */
    Set<ProcessorDTO> getProcessors(String groupId);

    /**
     * Verifies the specified processor can be updated.
     *
     * @param processorDTO
     */
    void verifyUpdateProcessor(ProcessorDTO processorDTO);

    /**
     * Verifies the specified processor can be updated.
     *
     * @param groupId
     * @param processorDTO
     */
    void verifyUpdateProcessor(String groupId, ProcessorDTO processorDTO);

    /**
     * Updates the specified Processor.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param processorDTO The processorDTO
     * @return The updated processor
     */
    ConfigurationSnapshot<ProcessorDTO> updateProcessor(Revision revision, String groupId, ProcessorDTO processorDTO);

    /**
     * Verifies the specified processor can be removed.
     *
     * @param groupId
     * @param processorId
     */
    void verifyDeleteProcessor(String groupId, String processorId);

    /**
     * Deletes the specified processor.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param processorId The processor id to delete
     * @return 
     */
    ConfigurationSnapshot<Void> deleteProcessor(Revision revision, String groupId, String processorId);

    // ----------------------------------------
    // Connections methods
    // ----------------------------------------
    /**
     * Gets the Connection transfer objects for the specified source processor.
     *
     * @param groupId
     * @return The Connection transfer objects
     */
    Set<ConnectionDTO> getConnections(String groupId);

    /**
     * Gets the specified Connection transfer object.
     *
     * @param groupId
     * @param connectionId The ID of the connection
     * @return The Connection transfer object
     */
    ConnectionDTO getConnection(String groupId, String connectionId);

    /**
     * Gets the status history of the specified connection.
     *
     * @param groupId
     * @param connectionId
     * @return
     */
    StatusHistoryDTO getConnectionStatusHistory(String groupId, String connectionId);

    /**
     * Creates a new Relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param connectionDTO The Connection DTO
     * @return The Connection DTO
     */
    ConfigurationSnapshot<ConnectionDTO> createConnection(Revision revision, String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be created.
     *
     * @param groupId
     * @param connectionDTO
     */
    void verifyCreateConnection(String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be updated.
     *
     * @param groupId
     * @param connectionDTO
     */
    void verifyUpdateConnection(String groupId, ConnectionDTO connectionDTO);

    /**
     * Updates the specified Relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param connectionDTO The Connection DTO
     * @return The Connection DTO
     */
    ConfigurationSnapshot<ConnectionDTO> updateConnection(Revision revision, String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be removed.
     *
     * @param groupId
     * @param connectionId
     */
    void verifyDeleteConnection(String groupId, String connectionId);

    /**
     * Deletes the specified relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param connectionId The ID of the connection
     * @return 
     */
    ConfigurationSnapshot<Void> deleteConnection(Revision revision, String groupId, String connectionId);

    // ----------------------------------------
    // InputPort methods
    // ----------------------------------------
    /**
     * Creates a new input port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the group this port should be create in
     * @param inputPortDTO The input PortDTO
     * @return
     */
    ConfigurationSnapshot<PortDTO> createInputPort(Revision revision, String groupId, PortDTO inputPortDTO);

    /**
     * Gets an input port.
     *
     * @param groupId The id of the group this port is in
     * @param inputPortId The input port id
     * @return
     */
    PortDTO getInputPort(String groupId, String inputPortId);

    /**
     * Gets all input ports in a given group.
     *
     * @param groupId The id of the group
     * @return
     */
    Set<PortDTO> getInputPorts(String groupId);

    /**
     * Determines if the input port could be updated.
     *
     * @param groupId The id of the group
     * @param inputPortDTO The id of the input port
     */
    void verifyUpdateInputPort(String groupId, PortDTO inputPortDTO);

    /**
     * Updates the specified input port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the group
     * @param inputPortDTO The input PortDTO
     * @return
     */
    ConfigurationSnapshot<PortDTO> updateInputPort(Revision revision, String groupId, PortDTO inputPortDTO);

    /**
     * Determines if the input port could be deleted.
     *
     * @param groupId The id of the group
     * @param inputPortId The id of the input port
     */
    void verifyDeleteInputPort(String groupId, String inputPortId);

    /**
     * Deletes the specified input port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the group
     * @param inputPortId The id of the input port
     * @return
     */
    ConfigurationSnapshot<Void> deleteInputPort(Revision revision, String groupId, String inputPortId);

    // ----------------------------------------
    // OutputPort methods
    // ----------------------------------------
    /**
     * Creates a new output port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the group this port should be create in
     * @param outputPortDTO The output PortDTO
     * @return
     */
    ConfigurationSnapshot<PortDTO> createOutputPort(Revision revision, String groupId, PortDTO outputPortDTO);

    /**
     * Gets an output port.
     *
     * @param groupId The id of the group this port is in
     * @param outputPortId The output port id
     * @return
     */
    PortDTO getOutputPort(String groupId, String outputPortId);

    /**
     * Gets all output ports in a given group.
     *
     * @param groupId The id of the group
     * @return
     */
    Set<PortDTO> getOutputPorts(String groupId);

    /**
     * Determines if the output port could be updated.
     *
     * @param groupId The id of the group
     * @param outputPortDTO The id of the output port
     */
    void verifyUpdateOutputPort(String groupId, PortDTO outputPortDTO);

    /**
     * Updates the specified output port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the group
     * @param outputPortDTO The output PortDTO
     * @return
     */
    ConfigurationSnapshot<PortDTO> updateOutputPort(Revision revision, String groupId, PortDTO outputPortDTO);

    /**
     * Determines if the output port could be deleted.
     *
     * @param groupId The id of the group
     * @param outputPortId The id of the output port
     */
    void verifyDeleteOutputPort(String groupId, String outputPortId);

    /**
     * Determines if the output port could be deleted.
     *
     * @param revision
     * @param groupId The id of the group
     * @param outputPortId The id of the output port
     * @return 
     */
    ConfigurationSnapshot<Void> deleteOutputPort(Revision revision, String groupId, String outputPortId);

    // ----------------------------------------
    // ProcessGroup methods
    // ----------------------------------------
    /**
     * Creates a new process group.
     *
     * @param parentGroupId The id of the parent group
     * @param revision Revision to compare with current base revision
     * @param processGroupDTO The ProcessGroupDTO
     * @return
     */
    ConfigurationSnapshot<ProcessGroupDTO> createProcessGroup(String parentGroupId, Revision revision, ProcessGroupDTO processGroupDTO);

    /**
     * Returns the process group.
     *
     * @param groupId
     * @param recurse
     * @return ProcessGroup transfer object
     */
    ConfigurationSnapshot<ProcessGroupDTO> getProcessGroup(String groupId, boolean recurse);

    /**
     * Gets all process groups in the specified parent group.
     *
     * @param parentGroupId The id of the parent group
     * @return
     */
    Set<ProcessGroupDTO> getProcessGroups(String parentGroupId);

    /**
     * Verifies the specified process group can be updated.
     *
     * @param processGroupDTO The ProcessGroupDTO
     */
    void verifyUpdateProcessGroup(ProcessGroupDTO processGroupDTO);

    /**
     * Updates the specified process group.
     *
     * @param revision Revision to compare with current base revision
     * @param parentGroupId The id of the parent group
     * @param processGroupDTO The ProcessGroupDTO
     * @return
     */
    ConfigurationSnapshot<ProcessGroupDTO> updateProcessGroup(Revision revision, String parentGroupId, ProcessGroupDTO processGroupDTO);

    /**
     * Verifies the specified process group can be removed.
     *
     * @param groupId The id of the process group
     */
    void verifyDeleteProcessGroup(String groupId);

    /**
     * Deletes the specified process group.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the process group
     * @return
     */
    ConfigurationSnapshot<Void> deleteProcessGroup(Revision revision, String groupId);

    /**
     * The instance id of this NiFi.
     *
     * @return
     */
    String getInstanceId();

    // ----------------------------------------
    // RemoteProcessGroup methods
    // ----------------------------------------
    /**
     * Creates a new remote process group.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the parent group
     * @param remoteProcessGroupDTO The RemoteProcessGroupDTO
     * @return
     */
    ConfigurationSnapshot<RemoteProcessGroupDTO> createRemoteProcessGroup(Revision revision, String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO);

    /**
     * Gets a remote process group.
     *
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @return
     */
    RemoteProcessGroupDTO getRemoteProcessGroup(String groupId, String remoteProcessGroupId);

    /**
     * Gets all remote process groups in the a given parent group.
     *
     * @param groupId The id of the parent group
     * @return
     */
    Set<RemoteProcessGroupDTO> getRemoteProcessGroups(String groupId);

    /**
     * Gets the remote process group status history.
     *
     * @param groupId The id of the parent group
     * @param id The id of the remote process group
     * @return
     */
    StatusHistoryDTO getRemoteProcessGroupStatusHistory(String groupId, String id);

    /**
     * Verifies the specified remote process group can be updated.
     *
     * @param groupId The id of the parent group
     * @param remoteProcessGroupDTO The RemoteProcessGroupDTO
     */
    void verifyUpdateRemoteProcessGroup(String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO);

    /**
     * Verifies the specified remote process group can update the specified
     * remote input port.
     *
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     */
    void verifyUpdateRemoteProcessGroupInputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Verifies the specified remote process group can update the specified
     * remote output port.
     *
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     */
    void verifyUpdateRemoteProcessGroupOutputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Updates the specified remote process group.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the parent group
     * @param remoteProcessGroupDTO The RemoteProcessGroupDTO
     * @return
     */
    ConfigurationSnapshot<RemoteProcessGroupDTO> updateRemoteProcessGroup(Revision revision, String groupId,
            RemoteProcessGroupDTO remoteProcessGroupDTO);

    /**
     * Updates the specified remote process groups input port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     * @return
     */
    ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupInputPort(Revision revision, String groupId, String remoteProcessGroupId,
            RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Updates the specified remote process groups output port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     * @return
     */
    ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupOutputPort(Revision revision, String groupId, String remoteProcessGroupId,
            RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Verifies the remote process group can be deleted.
     *
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     */
    void verifyDeleteRemoteProcessGroup(String groupId, String remoteProcessGroupId);

    /**
     * Deletes the specified remote process group.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @return
     */
    ConfigurationSnapshot<Void> deleteRemoteProcessGroup(Revision revision, String groupId, String remoteProcessGroupId);

    // ----------------------------------------
    // Funnel methods
    // ----------------------------------------
    /**
     * Creates a funnel.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param funnelDTO
     * @return The funnel DTO
     */
    ConfigurationSnapshot<FunnelDTO> createFunnel(Revision revision, String groupId, FunnelDTO funnelDTO);

    /**
     * Gets the specified funnel.
     *
     * @param groupId
     * @param funnelId The funnel id
     * @return The funnel transfer object
     */
    FunnelDTO getFunnel(String groupId, String funnelId);

    /**
     * Gets all of the funnels.
     *
     * @param groupId
     * @return The funnel transfer objects
     */
    Set<FunnelDTO> getFunnels(String groupId);

    /**
     * Updates the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param funnelDTO The funnel DTO
     * @return The funnel DTO
     */
    ConfigurationSnapshot<FunnelDTO> updateFunnel(Revision revision, String groupId, FunnelDTO funnelDTO);

    /**
     * Verifies the specified funnel can be deleted.
     *
     * @param groupId
     * @param funnelId
     */
    void verifyDeleteFunnel(String groupId, String funnelId);

    /**
     * Deletes the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param funnelId The funnel id
     * @return 
     */
    ConfigurationSnapshot<Void> deleteFunnel(Revision revision, String groupId, String funnelId);

    // ----------------------------------------
    // Label methods
    // ----------------------------------------
    /**
     * Creates a label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param labelDTO The label DTO
     * @return The label DTO
     */
    ConfigurationSnapshot<LabelDTO> createLabel(Revision revision, String groupId, LabelDTO labelDTO);

    /**
     * Gets the specified label.
     *
     * @param groupId
     * @param labelId The label id
     * @return The label transfer object
     */
    LabelDTO getLabel(String groupId, String labelId);

    /**
     * Gets all of the labels.
     *
     * @param groupId
     * @return The label transfer objects
     */
    Set<LabelDTO> getLabels(String groupId);

    /**
     * Updates the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param labelDTO The label DTO
     * @return The label DTO
     */
    ConfigurationSnapshot<LabelDTO> updateLabel(Revision revision, String groupId, LabelDTO labelDTO);

    /**
     * Deletes the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId
     * @param labelId The label id
     * @return 
     */
    ConfigurationSnapshot<Void> deleteLabel(Revision revision, String groupId, String labelId);

    // ----------------------------------------
    // History methods
    // ----------------------------------------
    /**
     * Returns actions that meet the specified criteria.
     *
     * @param historyQuery
     * @return
     */
    HistoryDTO getActions(HistoryQueryDTO historyQuery);

    /**
     * Returns the details for the specified action id.
     *
     * @param actionId
     * @return
     */
    ActionDTO getAction(Integer actionId);

    /**
     * Purges all actions up to the specified end date.
     *
     * @param endDate The end date
     */
    void deleteActions(Date endDate);

    /**
     * Gets the history for the specified property for the specified processor.
     *
     * @param processorId
     * @return
     */
    ProcessorHistoryDTO getProcessorHistory(String processorId);

    // ----------------------------------------
    // Snippet methods
    // ----------------------------------------
    /**
     * Creates a new snippet based off the existing snippet.
     *
     * @param revision
     * @param groupId
     * @param snippetId
     * @param originX
     * @param originY
     * @return
     */
    ConfigurationSnapshot<FlowSnippetDTO> copySnippet(Revision revision, String groupId, String snippetId, Double originX, Double originY);

    /**
     * Creates a new snippet.
     *
     * @param revision
     * @param snippet
     * @return
     */
    ConfigurationSnapshot<SnippetDTO> createSnippet(Revision revision, SnippetDTO snippet);

    /**
     * Gets the specified snippet.
     *
     * @param snippetId
     * @return
     */
    SnippetDTO getSnippet(String snippetId);

    /**
     * Determines if this snippet can be updated.
     *
     * @param snippetDto
     */
    void verifyUpdateSnippet(SnippetDTO snippetDto);

    /**
     * If group id is specified, moves the specified snippet to the specified
     * group.
     *
     * @param revision
     * @param snippetDto
     * @return
     */
    ConfigurationSnapshot<SnippetDTO> updateSnippet(Revision revision, SnippetDTO snippetDto);

    /**
     * Determines if this snippet can be removed.
     *
     * @param id
     */
    void verifyDeleteSnippet(String id);

    /**
     * Removes the specified snippet.
     *
     * @param revision
     * @param snippetId
     * @return
     */
    ConfigurationSnapshot<Void> deleteSnippet(Revision revision, String snippetId);

    // ----------------------------------------
    // User methods
    // ----------------------------------------
    /**
     * Gets the user with the specified id.
     *
     * @param userId The user id
     * @return
     */
    UserDTO getUser(String userId);

    /**
     * Gets all of the users registered with this controller.
     *
     * @param grouped
     * @return
     */
    Collection<UserDTO> getUsers(Boolean grouped);

    /**
     * Updates the specified user accordingly.
     *
     * @param user The user to update
     * @return
     */
    UserDTO updateUser(UserDTO user);

    /**
     * Invalidates the specified user.
     *
     * @param userId
     */
    void invalidateUser(String userId);

    /**
     * Invalidates the specified user accounts and all accounts associated with
     * this group.
     *
     * @param userGroup
     * @param userIds
     */
    void invalidateUserGroup(String userGroup, Set<String> userIds);

    /**
     * Deletes the specified user.
     *
     * @param userId
     */
    void deleteUser(String userId);

    /**
     * Updates a user group with the specified group and comprised of the
     * specified users.
     *
     * @param userGroup
     * @return 
     */
    UserGroupDTO updateUserGroup(UserGroupDTO userGroup);

    /**
     * Ungroups the specified user.
     * @param userId
     */
    void removeUserFromGroup(String userId);

    /**
     * Deletes the specified user group.
     * @param userGroup
     */
    void removeUserGroup(String userGroup);

    // ----------------------------------------
    // Cluster methods
    // ----------------------------------------
    /**
     * @return true if controller is connected or trying to connect to the
     * cluster
     */
    boolean isClustered();

    /**
     * Gets the id of this node, if clustered. If not clustered, returns null.
     *
     * @return
     */
    String getNodeId();

    /**
     * Returns the contents of cluster.
     *
     * @return the contents of cluster
     */
    ClusterDTO getCluster();

    /**
     * Returns the contents of the node.
     *
     * @param nodeId a node identifier
     * @return the contents of the node
     */
    NodeDTO getNode(String nodeId);

    /**
     * Updates the contents of the node.
     *
     * @param nodeDTO a node transfer object
     * @return 
     */
    NodeDTO updateNode(NodeDTO nodeDTO);

    /**
     * Deletes the node.
     *
     * @param nodeId a node identifier
     */
    void deleteNode(String nodeId);

    /**
     * Returns the status the specified node id.
     *
     * @param nodeId The id of the desired node
     * @return The node status
     */
    NodeStatusDTO getNodeStatus(String nodeId);

    /**
     * Returns the system diagnostics for the specified node id.
     *
     * @param nodeId The id of the desired node
     * @return The node status
     */
    NodeSystemDiagnosticsDTO getNodeSystemDiagnostics(String nodeId);

    /**
     * Returns the cluster's status.
     *
     * @return The cluster status
     */
    ClusterStatusDTO getClusterStatus();

    /**
     * Returns a processor's status for each node connected to the cluster.
     *
     * @param processorId a processor identifier
     * @return The cluster processor status transfer object.
     */
    ClusterProcessorStatusDTO getClusterProcessorStatus(String processorId);

    /**
     * Returns the processor status history for each node connected to the
     * cluster.
     *
     * @param processorId
     * @return
     */
    ClusterStatusHistoryDTO getClusterProcessorStatusHistory(String processorId);

    /**
     * Returns a connection's status for each node connected to the cluster.
     *
     * @param connectionId a connection identifier
     * @return The cluster connection status transfer object.
     */
    ClusterConnectionStatusDTO getClusterConnectionStatus(String connectionId);

    /**
     * Returns the connection status history for each node connected to the
     * cluster.
     *
     * @param connectionId
     * @return
     */
    ClusterStatusHistoryDTO getClusterConnectionStatusHistory(String connectionId);

    /**
     * Returns the process group status history for each node connected to the
     * cluster.
     *
     * @param processGroupId
     * @return
     */
    ClusterStatusHistoryDTO getClusterProcessGroupStatusHistory(String processGroupId);

    /**
     * Returns the remote process group status history for each node connected
     * to the cluster.
     *
     * @param remoteProcessGroupId
     * @return
     */
    ClusterStatusHistoryDTO getClusterRemoteProcessGroupStatusHistory(String remoteProcessGroupId);

    /**
     * Returns a remote process group's status for each node connected to the
     * cluster.
     *
     * @param remoteProcessGroupId a remote process group identifier
     * @return The cluster remote process group status transfer object.
     */
    ClusterRemoteProcessGroupStatusDTO getClusterRemoteProcessGroupStatus(String remoteProcessGroupId);

    /**
     * Returns an input port's status for each node connected to the cluster.
     *
     * @param inputPortId a port identifier
     * @return The cluster port status transfer object.
     */
    ClusterPortStatusDTO getClusterInputPortStatus(String inputPortId);

    /**
     * Returns an output port's status for each node connected to the cluster.
     *
     * @param outputPortId a port identifier
     * @return The cluster port status transfer object.
     */
    ClusterPortStatusDTO getClusterOutputPortStatus(String outputPortId);

    // ----------------------------------------
    // BulletinBoard methods
    // ----------------------------------------   
    /**
     * Returns the bulletin board for this NiFi.
     *
     * @param query
     * @return
     */
    BulletinBoardDTO getBulletinBoard(BulletinQueryDTO query);

    // ----------------------------------------
    // System diagnostics methods
    // ----------------------------------------  
    /**
     * Returns the system diagnostics.
     *
     * @return
     */
    SystemDiagnosticsDTO getSystemDiagnostics();
}
