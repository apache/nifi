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
package org.apache.nifi.groups;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.components.VersionedComponent;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.remote.RemoteGroupPort;

/**
 * <p>
 * ProcessGroup objects are containers for processing entities, such as
 * {@link Processor}s, {@link Port}s, and other {@link ProcessGroup}s.
 * </p>
 *
 * <p>
 * MUST BE THREAD-SAFE</p>
 */
public interface ProcessGroup extends ComponentAuthorizable, Positionable, VersionedComponent {

    /**
     * Predicate for starting eligible Processors.
     */
    Predicate<ProcessorNode> START_PROCESSORS_FILTER = node -> !node.isRunning() && !ScheduledState.DISABLED.equals(node.getScheduledState()) && node.getValidationStatus() == ValidationStatus.VALID;

    /**
     * Predicate for stopping eligible Processors.
     */
    Predicate<ProcessorNode> STOP_PROCESSORS_FILTER = node -> node.isRunning();

    /**
     * Predicate for enabling eligible Processors.
     */
    Predicate<ProcessorNode> ENABLE_PROCESSORS_FILTER = node -> ScheduledState.DISABLED.equals(node.getScheduledState());

    /**
     * Predicate for disabling eligible Processors.
     */
    Predicate<ProcessorNode> DISABLE_PROCESSORS_FILTER = node -> !node.isRunning() && !ScheduledState.DISABLED.equals(node.getScheduledState());

    /**
     * Predicate for starting eligible Ports.
     */
    Predicate<Port> START_PORTS_FILTER = port -> !port.isRunning() && !ScheduledState.DISABLED.equals(port.getScheduledState()) && port.isValid();

    /**
     * Predicate for stopping eligible Ports.
     */
    Predicate<Port> STOP_PORTS_FILTER = port -> ScheduledState.RUNNING.equals(port.getScheduledState());

    /**
     * Predicate for enabling eligible Processors.
     */
    Predicate<Port> ENABLE_PORTS_FILTER = port -> ScheduledState.DISABLED.equals(port.getScheduledState());

    /**
     * Predicate for disabling eligible Ports.
     */
    Predicate<Port> DISABLE_PORTS_FILTER = port -> !port.isRunning() && !ScheduledState.DISABLED.equals(port.getScheduledState());

    /**
     * @return a reference to this ProcessGroup's parent. This will be
     * <tt>null</tt> if and only if this is the root group.
     */
    ProcessGroup getParent();

    /**
     * Updates the ProcessGroup to point to a new parent
     *
     * @param group new parent group
     */
    void setParent(ProcessGroup group);

    /**
     * @return the ID of the ProcessGroup
     */
    @Override
    String getIdentifier();

    /**
     * @return the name of the ProcessGroup
     */
    String getName();

    /**
     * Updates the name of this ProcessGroup.
     *
     * @param name new name
     */
    void setName(String name);

    /**
     * @return the user-set comments about this ProcessGroup, or
     * <code>null</code> if no comments have been set
     */
    String getComments();

    /**
     * Updates the comments for this ProcessGroup
     *
     * @param comments new comments
     */
    void setComments(String comments);

    /**
     * @return the counts for this ProcessGroup
     */
    ProcessGroupCounts getCounts();

    /**
     * Starts all Processors, Local Ports, and Funnels that are directly within
     * this group and any child ProcessGroups, except for those that are
     * disabled.
     */
    void startProcessing();

    /**
     * Stops all Processors, Local Ports, and Funnels that are directly within
     * this group and child ProcessGroups, except for those that are disabled.
     */
    void stopProcessing();

    /**
     * Enables the given Processor
     *
     * @param processor the processor to start
     * @throws IllegalStateException if the processor is not valid, or is
     * already running
     */
    void enableProcessor(ProcessorNode processor);

    /**
     * Enables the given Input Port
     *
     * @param port to enable
     */
    void enableInputPort(Port port);

    /**
     * Enables the given Output Port
     *
     * @param port to enable
     */
    void enableOutputPort(Port port);

    /**
     * Starts the given Processor
     *
     * @param processor the processor to start
     * @param failIfStopping If <code>false</code>, and the Processor is in the 'STOPPING' state,
     *            then the Processor will automatically restart itself as soon as its last thread finishes. If this
     *            value is <code>true</code> or if the Processor is in any state other than 'STOPPING' or 'RUNNING', then this method
     *            will throw an {@link IllegalStateException}.
     * @throws IllegalStateException if the processor is not valid, or is
     *             already running
     */
    CompletableFuture<Void> startProcessor(ProcessorNode processor, boolean failIfStopping);

    /**
     * Starts the given Input Port
     *
     * @param port to start
     */
    void startInputPort(Port port);

    /**
     * Starts the given Output Port
     *
     * @param port to start
     */
    void startOutputPort(Port port);

    /**
     * Starts the given Funnel
     *
     * @param funnel to start
     */
    void startFunnel(Funnel funnel);

    /**
     * Stops the given Processor
     *
     * @param processor to stop
     */
    CompletableFuture<Void> stopProcessor(ProcessorNode processor);

    /**
     * Terminates the given Processor
     *
     * @param processor processor to Terminate
     * @throws IllegalStateException if the Processor's Scheduled State is not STOPPED.
     */
    void terminateProcessor(ProcessorNode processor);

    /**
     * Stops the given Port
     *
     * @param port to stop
     */
    void stopInputPort(Port port);

    /**
     * Stops the given Port
     *
     * @param port to stop
     */
    void stopOutputPort(Port port);

    /**
     * Disables the given Processor
     *
     * @param processor the processor to start
     * @throws IllegalStateException if the processor is not valid, or is
     * already running
     */
    void disableProcessor(ProcessorNode processor);

    /**
     * Disables the given Input Port
     *
     * @param port to disable
     */
    void disableInputPort(Port port);

    /**
     * Disables the given Output Port
     *
     * @param port to disable
     */
    void disableOutputPort(Port port);

    /**
     * Indicates that the Flow is being shutdown; allows cleanup of resources
     * associated with processors, etc.
     */
    void shutdown();

    /**
     * @return a boolean indicating whether or not this ProcessGroup is the root
     * group
     */
    boolean isRootGroup();

    /**
     * Adds a {@link Port} to be used for transferring {@link FlowFile}s from
     * external sources to {@link Processor}s and other {@link Port}s within
     * this ProcessGroup.
     *
     * @param port to add
     */
    void addInputPort(Port port);

    /**
     * Removes a {@link Port} from this ProcessGroup's list of Input Ports.
     *
     * @param port the Port to remove
     * @throws NullPointerException if <code>port</code> is null
     * @throws IllegalStateException if port is not an Input Port for this
     * ProcessGroup
     */
    void removeInputPort(Port port);

    /**
     * @return the {@link Set} of all {@link Port}s that are used by this
     * ProcessGroup as Input Ports.
     */
    Set<Port> getInputPorts();

    /**
     * @param id the ID of the input port
     * @return the input port with the given ID, or <code>null</code> if it does
     * not exist.
     */
    Port getInputPort(String id);

    /**
     * Adds a {@link Port} to be used for transferring {@link FlowFile}s to
     * external sources.
     *
     * @param port the Port to add
     */
    void addOutputPort(Port port);

    /**
     * Removes a {@link Port} from this ProcessGroup's list of Output Ports.
     *
     * @param port the Port to remove
     * @throws NullPointerException if <code>port</code> is null
     * @throws IllegalStateException if port is not an Input Port for this
     * ProcessGroup
     */
    void removeOutputPort(Port port);

    /**
     * @param id the ID of the output port
     * @return the output port with the given ID, or <code>null</code> if it
     * does not exist.
     */
    Port getOutputPort(String id);

    /**
     * @return the {@link Set} of all {@link Port}s that are used by this
     * ProcessGroup as Output Ports.
     */
    Set<Port> getOutputPorts();

    /**
     * Adds a reference to a ProgressGroup as a child of this.
     *
     * @param group to add
     */
    void addProcessGroup(ProcessGroup group);

    /**
     * Returns the ProcessGroup whose parent is <code>this</code> and whose id
     * is given
     *
     * @param id identifier of group to get
     * @return child group
     */
    ProcessGroup getProcessGroup(String id);

    /**
     * @return a {@link Set} of all Process Group References that are contained
     * within this.
     */
    Set<ProcessGroup> getProcessGroups();

    /**
     * @param group the group to remove
     * @throws NullPointerException if <code>group</code> is null
     * @throws IllegalStateException if group is not member of this
     * ProcessGroup, or the given ProcessGroup is not empty (i.e., it contains
     * at least one Processor, ProcessGroup, Input Port, Output Port, or Label).
     */
    void removeProcessGroup(ProcessGroup group);

    /**
     * Adds the already constructed processor instance to this group
     *
     * @param processor the processor to add
     */
    void addProcessor(ProcessorNode processor);

    /**
     * Removes the given processor from this group, destroying the Processor.
     * The Processor is removed from the ProcessorRegistry, and any method in
     * the Processor that is annotated with the
     * {@link org.apache.nifi.processor.annotation.OnRemoved OnRemoved} annotation will be
     * invoked. All outgoing connections will also be destroyed
     *
     * @param processor the Processor to remove
     * @throws NullPointerException if <code>processor</code> is null
     * @throws IllegalStateException if <code>processor</code> is not a member
     * of this ProcessGroup, is currently running, or has any incoming
     * connections.
     */
    void removeProcessor(ProcessorNode processor);

    /**
     * @return a {@link Collection} of all FlowFileProcessors that are contained
     * within this.
     */
    Collection<ProcessorNode> getProcessors();

    /**
     * Returns the FlowFileProcessor with the given ID.
     *
     * @param id the ID of the processor to retrieve
     * @return the processor with the given ID
     * @throws NullPointerException if <code>id</code> is null.
     */
    ProcessorNode getProcessor(String id);

    /**
     * @param id the ID of the Connectable
     * @return the <code>Connectable</code> with the given ID, or
     * <code>null</code> if the <code>Connectable</code> is not a member of the
     * group
     */
    Connectable getConnectable(String id);

    /**
     * Adds the given connection to this ProcessGroup. This method also notifies
     * the Source and Destination of the Connection that the Connection has been
     * established.
     *
     * @param connection to add
     * @throws NullPointerException if the connection is null
     * @throws IllegalStateException if the source or destination of the
     * connection is not a member of this ProcessGroup or if a connection
     * already exists in this ProcessGroup with the same ID
     */
    void addConnection(Connection connection);

    /**
     * Removes the connection from this ProcessGroup.
     *
     * @param connection to remove
     * @throws IllegalStateException if <code>connection</code> is not contained
     * within this.
     */
    void removeConnection(Connection connection);

    /**
     * Inherits a Connection from another ProcessGroup; this does not perform
     * any validation but simply notifies the ProcessGroup that it is now the
     * owner of the given Connection. This is used in place of the
     * {@link #addConnection(Connection)} method when moving Connections from
     * one group to another because addConnection notifies both the Source and
     * Destination of the Connection that the Connection has been established;
     * this method does not notify either, as both the Source and Destination
     * should already be aware of the Connection.
     *
     * @param connection to inherit
     */
    void inheritConnection(Connection connection);

    /**
     * @param id identifier of connection
     * @return the Connection with the given ID, or <code>null</code> if the
     * connection does not exist.
     */
    Connection getConnection(String id);

    /**
     * @return the {@link Set} of all {@link Connection}s contained within this
     */
    Set<Connection> getConnections();

    /**
     * @param id of the Connection
     * @return the Connection with the given ID, if it exists as a child or
     * descendant of this ProcessGroup. This performs a recursive search of all
     * descendant ProcessGroups
     */
    Connection findConnection(String id);

    /**
     * @return a List of all Connections contains within this ProcessGroup and
     * any child ProcessGroups
     */
    List<Connection> findAllConnections();

    /**
     * @param id of the Funnel
     * @return the Funnel with the given ID, if it exists as a child or
     * descendant of this ProcessGroup. This performs a recursive search of all
     * descendant ProcessGroups
     */
    Funnel findFunnel(String id);

    /**
     * @param id of the Controller Service
     * @param includeDescendantGroups whether or not to include descendant process groups
     * @param includeAncestorGroups whether or not to include ancestor process groups
     * @return the Controller Service with the given ID
     */
    ControllerServiceNode findControllerService(String id, boolean includeDescendantGroups, boolean includeAncestorGroups);

    /**
     * @return a List of all Controller Services contained within this ProcessGroup and any child Process Groups
     */
    Set<ControllerServiceNode> findAllControllerServices();

    /**
     * Adds the given RemoteProcessGroup to this ProcessGroup
     *
     * @param remoteGroup group to add
     *
     * @throws NullPointerException if the given argument is null
     */
    void addRemoteProcessGroup(RemoteProcessGroup remoteGroup);

    /**
     * Removes the given RemoteProcessGroup from this ProcessGroup
     *
     * @param remoteGroup group to remove
     * @throws NullPointerException if the argument is null
     * @throws IllegalStateException if the given argument does not belong to
     * this ProcessGroup
     */
    void removeRemoteProcessGroup(RemoteProcessGroup remoteGroup);

    /**
     * @param id identifier of group to find
     * @return the RemoteProcessGroup that is the child of this ProcessGroup and
     * has the given ID. If no RemoteProcessGroup can be found with the given
     * ID, returns <code>null</code>
     */
    RemoteProcessGroup getRemoteProcessGroup(String id);

    /**
     * @return a set of all RemoteProcessGroups that belong to this
     * ProcessGroup. If no RemoteProcessGroup's have been added to this
     * ProcessGroup, will return an empty Set
     */
    Set<RemoteProcessGroup> getRemoteProcessGroups();

    /**
     * Adds the given Label to this ProcessGroup
     *
     * @param label the label to add
     *
     * @throws NullPointerException if the argument is null
     */
    void addLabel(Label label);

    /**
     * Removes the given Label from this ProcessGroup
     *
     * @param label the label to remove
     * @throws NullPointerException if the argument is null
     * @throws IllegalStateException if the given argument does not belong to
     * this ProcessGroup
     */
    void removeLabel(Label label);

    /**
     * @return a set of all Labels that belong to this ProcessGroup. If no
     * Labels belong to this ProcessGroup, returns an empty Set
     */
    Set<Label> getLabels();

    /**
     * @param id of the label
     * @return the Label that belongs to this ProcessGroup and has the given id.
     * If no Label can be found with this ID, returns <code>null</code>
     */
    Label getLabel(String id);

    /**
     * @param id of the group
     * @return the Process Group with the given ID, if it exists as a child of
     * this ProcessGroup, or is this ProcessGroup. This performs a recursive
     * search of all ProcessGroups and descendant ProcessGroups
     */
    ProcessGroup findProcessGroup(String id);

    /**
     * @return a List of all ProcessGroups that are children or descendants of this
     * ProcessGroup. This performs a recursive search of all descendant
     * ProcessGroups
     */
    List<ProcessGroup> findAllProcessGroups();

    /**
     * @param id of the group
     * @return the RemoteProcessGroup with the given ID, if it exists as a child
     * or descendant of this ProcessGroup. This performs a recursive search of
     * all ProcessGroups and descendant ProcessGroups
     */
    RemoteProcessGroup findRemoteProcessGroup(String id);

    /**
     * @return a List of all Remote Process Groups that are children or
     * descendants of this ProcessGroup. This performs a recursive search of all
     * descendant ProcessGroups
     */
    List<RemoteProcessGroup> findAllRemoteProcessGroups();

    /**
     * @param id of the processor node
     * @return the Processor with the given ID, if it exists as a child or
     * descendant of this ProcessGroup. This performs a recursive search of all
     * descendant ProcessGroups
     */
    ProcessorNode findProcessor(String id);

    /**
     * @return a List of all Processors that are children or descendants of this
     * ProcessGroup. This performs a recursive search of all descendant
     * ProcessGroups
     */
    List<ProcessorNode> findAllProcessors();

    /**
     * @param id of the Label
     * @return the Label with the given ID, if it exists as a child or
     * descendant of this ProcessGroup. This performs a recursive search of all
     * descendant ProcessGroups
     */
    Label findLabel(String id);

    /**
     * @return a List of all Labels that are children or descendants of this
     * ProcessGroup. This performsn a recursive search of all descendant
     * ProcessGroups
     */
    List<Label> findAllLabels();

    /**
     * @param id of the port
     * @return the input port with the given ID, if it exists; otherwise returns
     * null. This performs a recursive search of all Input Ports and descendant
     * ProcessGroups
     */
    Port findInputPort(String id);

    /**
     * @return a List of all InputPorts that are children or descendants of this
     * ProcessGroup. This performs a recursive search of all descendant
     * ProcessGroups
     */
    List<Port> findAllInputPorts();

    /**
     * @param name of port
     * @return the input port with the given name, if it exists; otherwise
     * returns null
     */
    Port getInputPortByName(String name);

    /**
     * @param id of the port
     * @return the output port with the given ID, if it exists; otherwise
     * returns null. This performs a recursive search of all Output Ports and
     * descendant ProcessGroups
     */
    Port findOutputPort(String id);

    /**
     * @return a List of all OutputPorts that are children or descendants of this
     * ProcessGroup. This performs a recursive search of all descendant
     * ProcessGroups
     */
    List<Port> findAllOutputPorts();

    /**
     * @param name of the port
     * @return the output port with the given name, if it exists; otherwise
     * returns null
     */
    Port getOutputPortByName(String name);

    /**
     * Adds the given funnel to this ProcessGroup and starts it. While other
     * components do not automatically start, the funnel does by default because
     * it is intended to be more of a notional component that users are unable
     * to explicitly start and stop. However, there is an override available in
     * {@link #addFunnel(Funnel, boolean)} because we may need to avoid starting
     * the funnel on restart until the flow is completely initialized.
     *
     * @param funnel to add
     */
    void addFunnel(Funnel funnel);

    /**
     * Adds the given funnel to this ProcessGroup and optionally starts the
     * funnel.
     *
     * @param funnel to add
     * @param autoStart true if should auto start
     */
    void addFunnel(Funnel funnel, boolean autoStart);

    /**
     * @return a Set of all Funnels that belong to this ProcessGroup
     */
    Set<Funnel> getFunnels();

    /**
     * @param id of the funnel
     * @return the funnel with the given identifier
     */
    Funnel getFunnel(String id);

    /**
     * Removes the given funnel from this ProcessGroup
     *
     * @param funnel to remove
     *
     * @throws IllegalStateException if the funnel is not a member of this
     * ProcessGroup or has incoming or outgoing connections
     */
    void removeFunnel(Funnel funnel);

    /**
     * @return a List of all Funnel that are children or descendants of this
     * ProcessGroup. This performs a recursive search of all descendant
     * ProcessGroups
     */
    List<Funnel> findAllFunnels();

    /**
     * Adds the given Controller Service to this group
     *
     * @param service the service to add
     */
    void addControllerService(ControllerServiceNode service);

    /**
     * Returns the controller service with the given id
     *
     * @param id the id of the controller service
     * @return the controller service with the given id, or <code>null</code> if no service exists with that id
     */
    ControllerServiceNode getControllerService(String id);

    /**
     * Returns a Set of all Controller Services that are available in this Process Group
     *
     * @param recursive if <code>true</code>, returns the Controller Services available to the parent Process Group, its parents, etc.
     * @return a Set of all Controller Services that are available in this Process Group
     */
    Set<ControllerServiceNode> getControllerServices(boolean recursive);

    /**
     * Removes the given Controller Service from this group
     *
     * @param service the service to remove
     */
    void removeControllerService(ControllerServiceNode service);

    /**
     * @return <code>true</code> if this ProcessGroup has no Processors, Labels,
     * Connections, ProcessGroups, RemoteProcessGroupReferences, or Ports.
     * Otherwise, returns <code>false</code>.
     */
    boolean isEmpty();

    /**
     * Removes all of the components whose ID's are specified within the given
     * {@link Snippet} from this ProcessGroup.
     *
     * @param snippet to remove
     *
     * @throws NullPointerException if argument is null
     * @throws IllegalStateException if any ID in the snippet refers to a
     * component that is not within this ProcessGroup
     */
    void remove(final Snippet snippet);

    /**
     * @param identifier of remote group port
     * @return the RemoteGroupPort with the given ID, if it exists; otherwise
     * returns null.
     */
    RemoteGroupPort findRemoteGroupPort(String identifier);

    /**
     * @return a Set of all {@link org.apache.nifi.connectable.Positionable}s contained within this
     * {@link ProcessGroup} and any child {@link ProcessGroup}s
     */
    Set<Positionable> findAllPositionables();

    /**
     * Moves all of the components whose ID's are specified within the given
     * {@link Snippet} from this ProcessGroup into the given destination
     * ProcessGroup
     *
     * @param snippet to move
     * @param destination where to move
     * @throws NullPointerException if either argument is null
     * @throws IllegalStateException if any ID in the snippet refers to a
     * component that is not within this ProcessGroup
     */
    void move(final Snippet snippet, final ProcessGroup destination);

    /**
     * Updates the Process Group to match the proposed flow
     *
     * @param proposedSnapshot the proposed flow
     * @param componentIdSeed a seed value to use when generating ID's for new components
     * @param verifyNotDirty whether or not to verify that the Process Group is not 'dirty'. If this value is <code>true</code>,
     *            and the Process Group has been modified since it was last synchronized with the Flow Registry, then this method will
     *            throw an IllegalStateException
     * @param updateSettings whether or not to update the process group's name and positions
     * @param updateDescendantVersionedFlows if a child/descendant Process Group is under Version Control, specifies whether or not to
     *            update the contents of that Process Group
     */
    void updateFlow(VersionedFlowSnapshot proposedSnapshot, String componentIdSeed, boolean verifyNotDirty, boolean updateSettings, boolean updateDescendantVersionedFlows);

    /**
     * Verifies a template with the specified name can be created.
     *
     * @param name name of the template
     */
    void verifyCanAddTemplate(String name);

    void verifyCanDelete();

    /**
     * Ensures that the ProcessGroup is eligible to be deleted.
     *
     * @param ignorePortConnections if true, the Connections that are currently connected to Ports
     * will be ignored. Otherwise, the ProcessGroup is not eligible for deletion if its input ports
     * or output ports have any connections
     *
     * @throws IllegalStateException if the ProcessGroup is not eligible for deletion
     */
    void verifyCanDelete(boolean ignorePortConnections);

    void verifyCanStart(Connectable connectable);

    void verifyCanStart();

    void verifyCanStop(Connectable connectable);

    void verifyCanStop();

    /**
     * Ensures that deleting the given snippet is a valid operation at this
     * point in time, depending on the state of this ProcessGroup
     *
     * @param snippet to delete
     *
     * @throws IllegalStateException if deleting the Snippet is not valid at
     * this time
     */
    void verifyCanDelete(Snippet snippet);

    /**
     * Ensure that moving the given snippet to the given new group is a valid
     * operation at this point in time, depending on the state of both
     * ProcessGroups
     *
     * @param snippet to move
     * @param newProcessGroup new location
     *
     * @throws IllegalStateException if the move is not valid at this time
     */
    void verifyCanMove(Snippet snippet, ProcessGroup newProcessGroup);

    /**
     * Ensures that the given variables can be updated
     *
     * @param updatedVariables the new set of variable names and values
     *
     * @throws IllegalStateException if one or more variables that are listed cannot be updated at this time
     */
    void verifyCanUpdateVariables(Map<String, String> updatedVariables);

    /**
     * Ensures that the contents of the Process Group can be update to match the given new flow
     *
     * @param updatedFlow the updated version of the flow
     * @param verifyConnectionRemoval whether or not to verify that connections that are not present in the updated flow can be removed
     * @param verifyNotDirty whether or not to verify that the Process Group is not 'dirty'. If <code>true</code> and the Process Group has been changed since
     *            it was last synchronized with the FlowRegistry, then this method will throw an IllegalStateException
     *
     * @throws IllegalStateException if the Process Group is not in a state that will allow the update
     */
    void verifyCanUpdate(VersionedFlowSnapshot updatedFlow, boolean verifyConnectionRemoval, boolean verifyNotDirty);

    /**
     * Ensures that the Process Group can have any local changes reverted
     *
     * @throws IllegalStateException if the Process Group is not in a state that will allow local changes to be reverted
     */
    void verifyCanRevertLocalModifications();

    /**
     * Ensures that the Process Group can have its local modifications shown
     *
     * @throws IllegalStateException if the Process Group is not in a state that will allow local modifications to be shown
     */
    void verifyCanShowLocalModifications();

    /**
     * Ensure that the contents of the Process Group can be saved to a Flow Registry in its current state
     *
     * @throws IllegalStateException if the Process Group cannot currently be saved to a Flow Registry
     */
    void verifyCanSaveToFlowRegistry(String registryId, String bucketId, String flowId);

    /**
     * Adds the given template to this Process Group
     *
     * @param template the template to add
     */
    void addTemplate(Template template);

    /**
     * Removes the given template from the Process Group
     *
     * @param template the template to remove
     */
    void removeTemplate(Template template);

    /**
     * Returns the template with the given ID
     *
     * @param id the ID of the template
     * @return the template with the given ID or <code>null</code> if no template
     *         exists in this Process Group with the given ID
     */
    Template getTemplate(String id);

    /**
     * @param id of the template
     * @return the Template with the given ID, if it exists as a child or
     *         descendant of this ProcessGroup. This performs a recursive search of all
     *         descendant ProcessGroups
     */
    Template findTemplate(String id);

    /**
     * @return a Set of all Templates that belong to this Process Group
     */
    Set<Template> getTemplates();

    /**
     * @return a Set of all Templates that belong to this Process Group and any descendant Process Groups
     */
    Set<Template> findAllTemplates();

    /**
     * Updates the variables that are provided by this Process Group
     *
     * @param variables the variables to provide
     * @throws IllegalStateException if the Process Group is not in a state that allows the variables to be updated
     */
    void setVariables(Map<String, String> variables);

    /**
     * Returns the Variable Registry for this Process Group
     *
     * @return the Variable Registry for this Process Group
     */
    ComponentVariableRegistry getVariableRegistry();

    /**
     * Returns a set of all components that are affected by the variable with the given name
     *
     * @param variableName the name of the variable
     * @return a set of all components that are affected by the variable with the given name
     */
    Set<ComponentNode> getComponentsAffectedByVariable(String variableName);

    /**
     * @return the version control information that indicates where this flow is stored in a Flow Registry,
     *         or <code>null</code> if this Process Group is not under version control.
     */
    VersionControlInformation getVersionControlInformation();

    /**
     * Updates the Version Control Information for this Process Group
     *
     * @param versionControlInformation specification of where the flow is tracked in Version Control
     * @param versionedComponentIds a mapping of component ID's to Versioned Component ID's. This is used to update the components in the
     *            Process Group so that the components that exist in the Process Group can be associated with the corresponding components in the
     *            Version Controlled flow
     */
    void setVersionControlInformation(VersionControlInformation versionControlInformation, Map<String, String> versionedComponentIds);

    /**
     * Disconnects this Process Group from version control. If not currently under version control, this method does nothing.
     */
    void disconnectVersionControl(boolean removeVersionedComponentIds);

    /**
     * Synchronizes the Process Group with the given Flow Registry, determining whether or not the local flow
     * is up to date with the newest version of the flow in the Registry and whether or not the local flow has been
     * modified since it was last synced with the Flow Registry. If this Process Group is not under Version Control,
     * this method will have no effect.
     *
     * @param flowRegistry the Flow Registry to synchronize with
     */
    void synchronizeWithFlowRegistry(FlowRegistryClient flowRegistry);

    /**
     * Called whenever a component within this group or the group itself is modified
     */
    void onComponentModified();
}
