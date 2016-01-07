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

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ComponentHistoryDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.NodeSystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ClusterConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterPortStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterRemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterStatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

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
     * @param query query
     * @return results
     */
    SearchResultsDTO searchController(String query);

    /**
     * Submits a provenance request.
     *
     * @param query query
     * @return results
     */
    ProvenanceDTO submitProvenance(ProvenanceDTO query);

    /**
     * Submits a new replay request.
     *
     * @param eventId id
     * @return event
     */
    ProvenanceEventDTO submitReplay(Long eventId);

    /**
     * Gets the content for the specified claim.
     *
     * @param eventId id
     * @param uri uri
     * @param contentDirection direction
     * @return content
     */
    DownloadableContent getContent(Long eventId, String uri, ContentDirection contentDirection);

    /**
     * Gets the content for the specified flowfile in the specified connection.
     *
     * @param groupId group
     * @param connectionId connection
     * @param flowfileUuid flowfile
     * @param uri uri
     * @return content
     */
    DownloadableContent getContent(String groupId, String connectionId, String flowfileUuid, String uri);

    /**
     * Retrieves provenance.
     *
     * @param queryId identifier
     * @return result
     */
    ProvenanceDTO getProvenance(String queryId);

    /**
     * Deletes provenance.
     *
     * @param queryId identifier
     */
    void deleteProvenance(String queryId);

    /**
     * Returns the available options for searching provenance.
     *
     * @return options
     */
    ProvenanceOptionsDTO getProvenanceSearchOptions();

    /**
     * Submits a lineage request.
     *
     * @param lineage lineage
     * @return lineage
     */
    LineageDTO submitLineage(LineageDTO lineage);

    /**
     * Gets the lineage with the specified id.
     *
     * @param lineageId id
     * @return lineage
     */
    LineageDTO getLineage(String lineageId);

    /**
     * Deletes the lineage with the specified id.
     *
     * @param lineageId lineage
     */
    void deleteLineage(String lineageId);

    /**
     * Gets the provenance event with the specified id.
     *
     * @param id id
     * @return event
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
     * @return snapshot
     */
    ConfigurationSnapshot<Void> createArchive(Revision revision);

    /**
     * Sets the annotation data for a processor.
     *
     * @param revision the revision to compare with current base revision
     * @param processorId the id of the processor to update
     * @param annotationData the annotation data
     *
     * @return snapshot
     */
    ConfigurationSnapshot<ProcessorDTO> setProcessorAnnotationData(Revision revision, String processorId, String annotationData);

    /**
     * Returns the process group status.
     *
     * @param groupId group
     * @return The process group status
     */
    ProcessGroupStatusDTO getProcessGroupStatus(String groupId);

    /**
     * Gets the process group status history.
     *
     * @param groupId id
     * @return history
     */
    StatusHistoryDTO getProcessGroupStatusHistory(String groupId);

    /**
     * Returns the controller status.
     *
     * @return status
     */
    ControllerStatusDTO getControllerStatus();

    /**
     * Updates the specified counter by setting its value to 0.
     *
     * @param counterId id
     * @return counter
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
     * Returns the list of controller service types.
     *
     * @param serviceType Filters only service types that implement this type
     * @return The list of available controller types
     */
    Set<DocumentedTypeDTO> getControllerServiceTypes(String serviceType);

    /**
     * Returns the list of reporting task types.
     *
     * @return The list of available reporting task types
     */
    Set<DocumentedTypeDTO> getReportingTaskTypes();

    /**
     * Returns the list of prioritizer types.
     *
     * @return The list of available prioritizer types
     */
    Set<DocumentedTypeDTO> getWorkQueuePrioritizerTypes();

    /**
     * Returns the current revision.
     *
     * @return revision
     */
    RevisionDTO getRevision();

    // ----------------------------------------
    // Template methods
    // ----------------------------------------
    /**
     * Creates a new Template based off the specified snippet.
     *
     * @param name name
     * @param description description
     * @param snippetId id
     * @return template
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
     * @param revision revision
     * @param groupId group id
     * @param templateId template id
     * @param originX x
     * @param originY y
     * @return snapshot
     */
    ConfigurationSnapshot<FlowSnippetDTO> createTemplateInstance(Revision revision, String groupId, Double originX, Double originY, String templateId);

    /**
     * Gets the template with the specified id.
     *
     * @param id id
     * @return template
     */
    TemplateDTO getTemplate(String id);

    /**
     * Gets the template, includes contents, with the specified id.
     *
     * @param id id
     * @return template
     */
    TemplateDTO exportTemplate(String id);

    /**
     * Gets all templates.
     *
     * @return templates
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
     * @param groupId group
     * @param id id
     * @return history
     */
    StatusHistoryDTO getProcessorStatusHistory(String groupId, String id);

    /**
     * Get the descriptor for the specified property of the specified processor.
     *
     * @param groupId group
     * @param id id
     * @param property property
     * @return descriptor
     */
    PropertyDescriptorDTO getProcessorPropertyDescriptor(String groupId, String id, String property);

    /**
     * Gets all the Processor transfer objects for this controller.
     *
     * @param groupId group
     * @return List of all the Processor transfer object
     */
    Set<ProcessorDTO> getProcessors(String groupId);

    /**
     * Verifies the specified processor can be updated.
     *
     * @param processorDTO processor
     */
    void verifyUpdateProcessor(ProcessorDTO processorDTO);

    /**
     * Verifies the specified processor can be updated.
     *
     * @param groupId group
     * @param processorDTO processor
     */
    void verifyUpdateProcessor(String groupId, ProcessorDTO processorDTO);

    /**
     * Updates the specified Processor.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param processorDTO The processorDTO
     * @return The updated processor
     */
    ConfigurationSnapshot<ProcessorDTO> updateProcessor(Revision revision, String groupId, ProcessorDTO processorDTO);

    /**
     * Verifies the specified processor can be removed.
     *
     * @param groupId group
     * @param processorId processor
     */
    void verifyDeleteProcessor(String groupId, String processorId);

    /**
     * Deletes the specified processor.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param processorId The processor id to delete
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteProcessor(Revision revision, String groupId, String processorId);

    // ----------------------------------------
    // Connections methods
    // ----------------------------------------
    /**
     * Gets the Connection transfer objects for the specified source processor.
     *
     * @param groupId group
     * @return The Connection transfer objects
     */
    Set<ConnectionDTO> getConnections(String groupId);

    /**
     * Gets the specified Connection transfer object.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @return The Connection transfer object
     */
    ConnectionDTO getConnection(String groupId, String connectionId);

    /**
     * Gets the status history of the specified connection.
     *
     * @param groupId group
     * @param connectionId connection
     * @return history
     */
    StatusHistoryDTO getConnectionStatusHistory(String groupId, String connectionId);

    /**
     * Creates a new Relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param connectionDTO The Connection DTO
     * @return The Connection DTO
     */
    ConfigurationSnapshot<ConnectionDTO> createConnection(Revision revision, String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be listed.
     *
     * @param groupId group
     * @param connectionId connection
     */
    void verifyListQueue(String groupId, String connectionId);

    /**
     * Determines if this connection can be created.
     *
     * @param groupId group
     * @param connectionDTO connection
     */
    void verifyCreateConnection(String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be updated.
     *
     * @param groupId group
     * @param connectionDTO connection
     */
    void verifyUpdateConnection(String groupId, ConnectionDTO connectionDTO);

    /**
     * Updates the specified Relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param connectionDTO The Connection DTO
     * @return The Connection DTO
     */
    ConfigurationSnapshot<ConnectionDTO> updateConnection(Revision revision, String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be removed.
     *
     * @param groupId group
     * @param connectionId connection
     */
    void verifyDeleteConnection(String groupId, String connectionId);

    /**
     * Deletes the specified relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param connectionId The ID of the connection
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteConnection(Revision revision, String groupId, String connectionId);

    /**
     * Creates a new flow file drop request.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @param dropRequestId The ID of the drop request
     * @return The DropRequest
     */
    DropRequestDTO createFlowFileDropRequest(String groupId, String connectionId, String dropRequestId);

    /**
     * Gets the specified flow file drop request.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @param dropRequestId The flow file drop request
     * @return The DropRequest
     */
    DropRequestDTO getFlowFileDropRequest(String groupId, String connectionId, String dropRequestId);

    /**
     * Cancels/removes the specified flow file drop request.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @param dropRequestId The flow file drop request
     * @return The DropRequest
     */
    DropRequestDTO deleteFlowFileDropRequest(String groupId, String connectionId, String dropRequestId);

    /**
     * Creates a new flow file listing request.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @param listingRequestId The ID of the listing request
     * @return The ListingRequest
     */
    ListingRequestDTO createFlowFileListingRequest(String groupId, String connectionId, String listingRequestId);

    /**
     * Gets a new flow file listing request.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @param listingRequestId The ID of the listing request
     * @return The ListingRequest
     */
    ListingRequestDTO getFlowFileListingRequest(String groupId, String connectionId, String listingRequestId);

    /**
     * Deletes a new flow file listing request.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @param listingRequestId The ID of the listing request
     * @return The ListingRequest
     */
    ListingRequestDTO deleteFlowFileListingRequest(String groupId, String connectionId, String listingRequestId);

    /**
     * Gets the specified flowfile from the specified connection.
     *
     * @param groupId group
     * @param connectionId The ID of the connection
     * @param flowFileUuid The UUID of the flowfile
     * @return The FlowFileDTO
     */
    FlowFileDTO getFlowFile(String groupId, String connectionId, String flowFileUuid);

    // ----------------------------------------
    // InputPort methods
    // ----------------------------------------
    /**
     * Creates a new input port.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the group this port should be create in
     * @param inputPortDTO The input PortDTO
     * @return snapshot
     */
    ConfigurationSnapshot<PortDTO> createInputPort(Revision revision, String groupId, PortDTO inputPortDTO);

    /**
     * Gets an input port.
     *
     * @param groupId The id of the group this port is in
     * @param inputPortId The input port id
     * @return port
     */
    PortDTO getInputPort(String groupId, String inputPortId);

    /**
     * Gets all input ports in a given group.
     *
     * @param groupId The id of the group
     * @return port
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
     * @return snapshort
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
     * @return snapshot
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
     * @return snapshot
     */
    ConfigurationSnapshot<PortDTO> createOutputPort(Revision revision, String groupId, PortDTO outputPortDTO);

    /**
     * Gets an output port.
     *
     * @param groupId The id of the group this port is in
     * @param outputPortId The output port id
     * @return port
     */
    PortDTO getOutputPort(String groupId, String outputPortId);

    /**
     * Gets all output ports in a given group.
     *
     * @param groupId The id of the group
     * @return ports
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
     * @return snapshot
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
     * @param revision revision
     * @param groupId The id of the group
     * @param outputPortId The id of the output port
     * @return snapshot
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
     * @return snapshot
     */
    ConfigurationSnapshot<ProcessGroupDTO> createProcessGroup(String parentGroupId, Revision revision, ProcessGroupDTO processGroupDTO);

    /**
     * Returns the process group.
     *
     * @param groupId group
     * @param recurse recurse
     * @return ProcessGroup transfer object
     */
    ConfigurationSnapshot<ProcessGroupDTO> getProcessGroup(String groupId, boolean recurse);

    /**
     * Gets all process groups in the specified parent group.
     *
     * @param parentGroupId The id of the parent group
     * @return process group
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
     * @return snapshot
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
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteProcessGroup(Revision revision, String groupId);

    /**
     * The instance id of this NiFi.
     *
     * @return identifier
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
     * @return snapshot
     */
    ConfigurationSnapshot<RemoteProcessGroupDTO> createRemoteProcessGroup(Revision revision, String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO);

    /**
     * Gets a remote process group.
     *
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @return group
     */
    RemoteProcessGroupDTO getRemoteProcessGroup(String groupId, String remoteProcessGroupId);

    /**
     * Gets all remote process groups in the a given parent group.
     *
     * @param groupId The id of the parent group
     * @return group
     */
    Set<RemoteProcessGroupDTO> getRemoteProcessGroups(String groupId);

    /**
     * Gets the remote process group status history.
     *
     * @param groupId The id of the parent group
     * @param id The id of the remote process group
     * @return history
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
     * Verifies the specified remote process group can update the specified remote input port.
     *
     * @param groupId The id of the parent group
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     */
    void verifyUpdateRemoteProcessGroupInputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Verifies the specified remote process group can update the specified remote output port.
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
     * @return snapshot
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
     * @return snapshot
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
     * @return snapshot
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
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteRemoteProcessGroup(Revision revision, String groupId, String remoteProcessGroupId);

    // ----------------------------------------
    // Funnel methods
    // ----------------------------------------
    /**
     * Creates a funnel.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param funnelDTO funnel
     * @return The funnel DTO
     */
    ConfigurationSnapshot<FunnelDTO> createFunnel(Revision revision, String groupId, FunnelDTO funnelDTO);

    /**
     * Gets the specified funnel.
     *
     * @param groupId group
     * @param funnelId The funnel id
     * @return The funnel transfer object
     */
    FunnelDTO getFunnel(String groupId, String funnelId);

    /**
     * Gets all of the funnels.
     *
     * @param groupId group
     * @return The funnel transfer objects
     */
    Set<FunnelDTO> getFunnels(String groupId);

    /**
     * Updates the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param funnelDTO The funnel DTO
     * @return The funnel DTO
     */
    ConfigurationSnapshot<FunnelDTO> updateFunnel(Revision revision, String groupId, FunnelDTO funnelDTO);

    /**
     * Verifies the specified funnel can be deleted.
     *
     * @param groupId group
     * @param funnelId funnel
     */
    void verifyDeleteFunnel(String groupId, String funnelId);

    /**
     * Deletes the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param funnelId The funnel id
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteFunnel(Revision revision, String groupId, String funnelId);

    // ----------------------------------------
    // Label methods
    // ----------------------------------------
    /**
     * Creates a label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param labelDTO The label DTO
     * @return The label DTO
     */
    ConfigurationSnapshot<LabelDTO> createLabel(Revision revision, String groupId, LabelDTO labelDTO);

    /**
     * Gets the specified label.
     *
     * @param groupId group
     * @param labelId The label id
     * @return The label transfer object
     */
    LabelDTO getLabel(String groupId, String labelId);

    /**
     * Gets all of the labels.
     *
     * @param groupId group
     * @return The label transfer objects
     */
    Set<LabelDTO> getLabels(String groupId);

    /**
     * Updates the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param labelDTO The label DTO
     * @return The label DTO
     */
    ConfigurationSnapshot<LabelDTO> updateLabel(Revision revision, String groupId, LabelDTO labelDTO);

    /**
     * Deletes the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId group
     * @param labelId The label id
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteLabel(Revision revision, String groupId, String labelId);

    // ----------------------------------------
    // Controller Services methods
    // ----------------------------------------
    /**
     * Creates a controller service.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service DTO
     */
    ConfigurationSnapshot<ControllerServiceDTO> createControllerService(Revision revision, ControllerServiceDTO controllerServiceDTO);

    /**
     * Gets all controller services.
     *
     * @return services
     */
    Set<ControllerServiceDTO> getControllerServices();

    /**
     * Gets the specified controller service.
     *
     * @param controllerServiceId id
     * @return service
     */
    ControllerServiceDTO getControllerService(String controllerServiceId);

    /**
     * Get the descriptor for the specified property of the specified controller service.
     *
     * @param id id
     * @param property property
     * @return property
     */
    PropertyDescriptorDTO getControllerServicePropertyDescriptor(String id, String property);

    /**
     * Gets the references for specified controller service.
     *
     * @param controllerServiceId id
     * @return service reference
     */
    Set<ControllerServiceReferencingComponentDTO> getControllerServiceReferencingComponents(String controllerServiceId);

    /**
     * Updates the referencing components for the specified controller service.
     *
     * @param revision revision
     * @param controllerServiceId id
     * @param scheduledState state
     * @param controllerServiceState the value of state
     * @return The referencing component dtos
     */
    ConfigurationSnapshot<Set<ControllerServiceReferencingComponentDTO>> updateControllerServiceReferencingComponents(
            Revision revision, String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);

    /**
     * Updates the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service DTO
     */
    ConfigurationSnapshot<ControllerServiceDTO> updateControllerService(Revision revision, ControllerServiceDTO controllerServiceDTO);

    /**
     * Deletes the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerServiceId The controller service id
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteControllerService(Revision revision, String controllerServiceId);

    /**
     * Verifies the specified controller service can be updated.
     *
     * @param controllerServiceDTO service
     */
    void verifyUpdateControllerService(ControllerServiceDTO controllerServiceDTO);

    /**
     * Verifies the referencing components of the specified controller service can be updated.
     *
     * @param controllerServiceId id
     * @param scheduledState schedule state
     * @param controllerServiceState service state
     */
    void verifyUpdateControllerServiceReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);

    /**
     * Verifies the specified controller service can be removed.
     *
     * @param controllerServiceId id
     */
    void verifyDeleteControllerService(String controllerServiceId);

    // ----------------------------------------
    // Reporting Task methods
    // ----------------------------------------
    /**
     * Creates a reporting task.
     *
     * @param revision Revision to compare with current base revision
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task DTO
     */
    ConfigurationSnapshot<ReportingTaskDTO> createReportingTask(Revision revision, ReportingTaskDTO reportingTaskDTO);

    /**
     * Gets all reporting tasks.
     *
     * @return tasks
     */
    Set<ReportingTaskDTO> getReportingTasks();

    /**
     * Gets the specified reporting task.
     *
     * @param reportingTaskId id
     * @return task
     */
    ReportingTaskDTO getReportingTask(String reportingTaskId);

    /**
     * Get the descriptor for the specified property of the specified reporting task.
     *
     * @param id id
     * @param property property
     * @return descriptor
     */
    PropertyDescriptorDTO getReportingTaskPropertyDescriptor(String id, String property);

    /**
     * Updates the specified reporting task.
     *
     * @param revision Revision to compare with current base revision
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task DTO
     */
    ConfigurationSnapshot<ReportingTaskDTO> updateReportingTask(Revision revision, ReportingTaskDTO reportingTaskDTO);

    /**
     * Deletes the specified reporting task.
     *
     * @param revision Revision to compare with current base revision
     * @param reportingTaskId The reporting task id
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteReportingTask(Revision revision, String reportingTaskId);

    /**
     * Verifies the specified reporting task can be updated.
     *
     * @param reportingTaskDTO task
     */
    void verifyUpdateReportingTask(ReportingTaskDTO reportingTaskDTO);

    /**
     * Verifies the specified reporting task can be removed.
     *
     * @param reportingTaskId id
     */
    void verifyDeleteReportingTask(String reportingTaskId);

    // ----------------------------------------
    // History methods
    // ----------------------------------------
    /**
     * Returns actions that meet the specified criteria.
     *
     * @param historyQuery query
     * @return history
     */
    HistoryDTO getActions(HistoryQueryDTO historyQuery);

    /**
     * Returns the details for the specified action id.
     *
     * @param actionId id
     * @return action
     */
    ActionDTO getAction(Integer actionId);

    /**
     * Purges all actions up to the specified end date.
     *
     * @param endDate The end date
     */
    void deleteActions(Date endDate);

    /**
     * Gets the history for the specified property for the specified component.
     *
     * @param componentId id
     * @return history
     */
    ComponentHistoryDTO getComponentHistory(String componentId);

    // ----------------------------------------
    // Snippet methods
    // ----------------------------------------
    /**
     * Creates a new snippet based off the existing snippet.
     *
     * @param revision revision
     * @param groupId group id
     * @param snippetId snippet id
     * @param originX x
     * @param originY y
     * @return snapshot
     */
    ConfigurationSnapshot<FlowSnippetDTO> copySnippet(Revision revision, String groupId, String snippetId, Double originX, Double originY);

    /**
     * Creates a new snippet.
     *
     * @param revision revision
     * @param snippet snippet
     * @return snapshot
     */
    ConfigurationSnapshot<SnippetDTO> createSnippet(Revision revision, SnippetDTO snippet);

    /**
     * Gets the specified snippet.
     *
     * @param snippetId id
     * @return snippet
     */
    SnippetDTO getSnippet(String snippetId);

    /**
     * Determines if this snippet can be updated.
     *
     * @param snippetDto snippet
     */
    void verifyUpdateSnippet(SnippetDTO snippetDto);

    /**
     * If group id is specified, moves the specified snippet to the specified group.
     *
     * @param revision revision
     * @param snippetDto snippet
     * @return snapshot
     */
    ConfigurationSnapshot<SnippetDTO> updateSnippet(Revision revision, SnippetDTO snippetDto);

    /**
     * Determines if this snippet can be removed.
     *
     * @param id id
     */
    void verifyDeleteSnippet(String id);

    /**
     * Removes the specified snippet.
     *
     * @param revision revision
     * @param snippetId snippet
     * @return snapshot
     */
    ConfigurationSnapshot<Void> deleteSnippet(Revision revision, String snippetId);

    // ----------------------------------------
    // User methods
    // ----------------------------------------
    /**
     * Gets the user with the specified id.
     *
     * @param userId The user id
     * @return user
     */
    UserDTO getUser(String userId);

    /**
     * Gets all of the users registered with this controller.
     *
     * @param grouped grouped
     * @return user
     */
    Collection<UserDTO> getUsers(Boolean grouped);

    /**
     * Creates a new account request.
     *
     * @return user
     */
    UserDTO createUser();

    /**
     * Updates the specified user accordingly.
     *
     * @param user The user to update
     * @return user
     */
    UserDTO updateUser(UserDTO user);

    /**
     * Invalidates the specified user.
     *
     * @param userId user
     */
    void invalidateUser(String userId);

    /**
     * Invalidates the specified user accounts and all accounts associated with this group.
     *
     * @param userGroup group
     * @param userIds id
     */
    void invalidateUserGroup(String userGroup, Set<String> userIds);

    /**
     * Deletes the specified user.
     *
     * @param userId user id
     */
    void deleteUser(String userId);

    /**
     * Updates a user group with the specified group and comprised of the specified users.
     *
     * @param userGroup group
     * @return group
     */
    UserGroupDTO updateUserGroup(UserGroupDTO userGroup);

    /**
     * Ungroups the specified user.
     *
     * @param userId id
     */
    void removeUserFromGroup(String userId);

    /**
     * Deletes the specified user group.
     *
     * @param userGroup group
     */
    void removeUserGroup(String userGroup);

    // ----------------------------------------
    // Cluster methods
    // ----------------------------------------
    /**
     * @return true if controller is connected or trying to connect to the cluster
     */
    boolean isClustered();

    /**
     * @return the id of this node, if clustered. If not clustered, returns null
     */
    String getNodeId();

    /**
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
     * @return node
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
     * @param processorId id
     * @return the processor status history for each node connected to the cluster
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
     * @param connectionId id
     * @return the connection status history for each node connected to the cluster
     */
    ClusterStatusHistoryDTO getClusterConnectionStatusHistory(String connectionId);

    /**
     * @param processGroupId id
     * @return the process group status history for each node connected to the cluster
     */
    ClusterStatusHistoryDTO getClusterProcessGroupStatusHistory(String processGroupId);

    /**
     * Returns a process group's status for each node connected to the cluster.
     *
     * @param processorId a process group identifier
     * @return The cluster process group status transfer object.
     */
    ClusterProcessGroupStatusDTO getClusterProcessGroupStatus(String processorId);

    /**
     * Returns the remote process group status history for each node connected to the cluster.
     *
     * @param remoteProcessGroupId a remote process group identifier
     * @return The cluster status history
     */
    ClusterStatusHistoryDTO getClusterRemoteProcessGroupStatusHistory(String remoteProcessGroupId);

    /**
     * Returns a remote process group's status for each node connected to the cluster.
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
     * @param query query
     * @return the bulletin board for this NiFi
     */
    BulletinBoardDTO getBulletinBoard(BulletinQueryDTO query);

    // ----------------------------------------
    // System diagnostics methods
    // ----------------------------------------
    /**
     * @return the system diagnostics
     */
    SystemDiagnosticsDTO getSystemDiagnostics();
}
