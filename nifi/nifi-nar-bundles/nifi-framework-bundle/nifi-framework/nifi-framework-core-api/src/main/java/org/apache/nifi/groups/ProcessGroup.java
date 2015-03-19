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
import java.util.Set;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Processor;

/**
 * <p>
 * ProcessGroup objects are containers for processing entities, such as
 * {@link Processor}s, {@link Port}s, and other {@link ProcessGroup}s.
 * </p>
 *
 * <p>
 * MUST BE THREAD-SAFE</p>
 */
public interface ProcessGroup {

    /**
     * @return a reference to this ProcessGroup's parent. This will be
     * <tt>null</tt> if and only if this is the root group.
     */
    ProcessGroup getParent();

    /**
     * Updates the ProcessGroup to point to a new parent
     *
     * @param group
     */
    void setParent(ProcessGroup group);

    /**
     * @return the ID of the ProcessGroup
     */
    String getIdentifier();

    /**
     * @return the name of the ProcessGroup
     */
    String getName();

    /**
     * Updates the name of this ProcessGroup.
     *
     * @param name
     */
    void setName(String name);

    /**
     * Updates the position of where this ProcessGroup is located in the graph
     */
    void setPosition(Position position);

    /**
     * Returns the position of where this ProcessGroup is located in the graph
     *
     * @return
     */
    Position getPosition();

    /**
     * @return the user-set comments about this ProcessGroup, or
     * <code>null</code> if no comments have been set
     */
    String getComments();

    /**
     * Updates the comments for this ProcessGroup
     *
     * @param comments
     */
    void setComments(String comments);

    /**
     * Returns the counts for this ProcessGroup
     *
     * @return
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
     * @param port
     */
    void enableInputPort(Port port);

    /**
     * Enables the given Output Port
     *
     * @param port
     */
    void enableOutputPort(Port port);


    /**
     * Starts the given Processor
     *
     * @param processor the processor to start
     * @throws IllegalStateException if the processor is not valid, or is
     * already running
     */
    void startProcessor(ProcessorNode processor);

    /**
     * Starts the given Input Port
     *
     * @param port
     */
    void startInputPort(Port port);

    /**
     * Starts the given Output Port
     *
     * @param port
     */
    void startOutputPort(Port port);

    /**
     * Starts the given Funnel
     *
     * @param funnel
     */
    void startFunnel(Funnel funnel);

    /**
     * Stops the given Processor
     *
     * @param processor
     */
    void stopProcessor(ProcessorNode processor);

    /**
     * Stops the given Port
     *
     * @param processor
     */
    void stopInputPort(Port port);

    /**
     * Stops the given Port
     *
     * @param processor
     */
    void stopOutputPort(Port port);

    /**
     * Stops the given Funnel
     *
     * @param processor
     */
    void stopFunnel(Funnel funnel);

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
     * @param port
     */
    void disableInputPort(Port port);

    /**
     * Disables the given Output Port
     *
     * @param port
     */
    void disableOutputPort(Port port);


    /**
     * Indicates that the Flow is being shutdown; allows cleanup of resources
     * associated with processors, etc.
     */
    void shutdown();

    /**
     * Returns a boolean indicating whether or not this ProcessGroup is the root
     * group
     *
     * @return
     */
    boolean isRootGroup();

    /**
     * Adds a {@link Port} to be used for transferring {@link FlowFile}s from
     * external sources to {@link Processor}s and other {@link Port}s within
     * this ProcessGroup.
     *
     * @param port
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
     * @return the newly created reference
     */
    void addProcessGroup(ProcessGroup group);

    /**
     * Returns the ProcessGroup whose parent is <code>this</code> and whose id
     * is given
     *
     * @param id
     * @return
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
     * {@link nifi.processor.annotation.OnRemoved OnRemoved} annotation will be
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
    Set<ProcessorNode> getProcessors();

    /**
     * Returns the FlowFileProcessor with the given ID.
     *
     * @param id the ID of the processor to retrieve
     * @return the processor with the given ID
     * @throws NullPointerException if <code>id</code> is null.
     */
    ProcessorNode getProcessor(String id);

    /**
     * Returns the <code>Connectable</code> with the given ID, or
     * <code>null</code> if the <code>Connectable</code> is not a member of the
     * group
     *
     * @param id the ID of the Connectable
     * @return
     */
    Connectable getConnectable(String id);

    /**
     * Adds the given connection to this ProcessGroup. This method also notifies
     * the Source and Destination of the Connection that the Connection has been
     * established.
     *
     * @param connection
     * @throws NullPointerException if the connection is null
     * @throws IllegalStateException if the source or destination of the
     * connection is not a member of this ProcessGroup or if a connection
     * already exists in this ProcessGroup with the same ID
     */
    void addConnection(Connection connection);

    /**
     * Removes the connection from this ProcessGroup.
     *
     * @param connection
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
     * @param connection
     */
    void inheritConnection(Connection connection);

    /**
     * @return the Connection with the given ID, or <code>null</code> if the
     * connection does not exist.
     */
    Connection getConnection(String id);

    /**
     * Returns the {@link Set} of all {@link Connection}s contained within this.
     *
     * @return
     */
    Set<Connection> getConnections();

    /**
     * Returns a List of all Connections contains within this ProcessGroup and
     * any child ProcessGroups.
     *
     * @return
     */
    List<Connection> findAllConnections();

    /**
     * Adds the given RemoteProcessGroup to this ProcessGroup
     *
     * @param remoteGroup
     *
     * @throws NullPointerException if the given argument is null
     */
    void addRemoteProcessGroup(RemoteProcessGroup remoteGroup);

    /**
     * Removes the given RemoteProcessGroup from this ProcessGroup
     *
     * @param remoteGroup
     * @throws NullPointerException if the argument is null
     * @throws IllegalStateException if the given argument does not belong to
     * this ProcessGroup
     */
    void removeRemoteProcessGroup(RemoteProcessGroup remoteGroup);

    /**
     * Returns the RemoteProcessGroup that is the child of this ProcessGroup and
     * has the given ID. If no RemoteProcessGroup can be found with the given
     * ID, returns <code>null</code>.
     *
     * @param id
     * @return
     */
    RemoteProcessGroup getRemoteProcessGroup(String id);

    /**
     * Returns a set of all RemoteProcessGroups that belong to this
     * ProcessGroup. If no RemoteProcessGroup's have been added to this
     * ProcessGroup, will return an empty Set.
     *
     * @return
     */
    Set<RemoteProcessGroup> getRemoteProcessGroups();

    /**
     * Adds the given Label to this ProcessGroup
     *
     * @param label the label to add
     * @return
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
     * Returns a set of all Labels that belong to this ProcessGroup. If no
     * Labels belong to this ProcessGroup, returns an empty Set.
     *
     * @return
     */
    Set<Label> getLabels();

    /**
     * Returns the Label that belongs to this ProcessGroup and has the given id.
     * If no Label can be found with this ID, returns <code>null</code>.
     *
     * @param id
     * @return
     */
    Label getLabel(String id);

    /**
     * Returns the Process Group with the given ID, if it exists as a child of
     * this ProcessGroup, or is this ProcessGroup. This performs a recursive
     * search of all ProcessGroups and descendant ProcessGroups
     *
     * @param id
     * @return
     */
    ProcessGroup findProcessGroup(String id);

    /**
     * Returns the RemoteProcessGroup with the given ID, if it exists as a child
     * or descendant of this ProcessGroup. This performs a recursive search of
     * all ProcessGroups and descendant ProcessGroups
     *
     * @param id
     * @return
     */
    RemoteProcessGroup findRemoteProcessGroup(String id);

    /**
     * Returns a List of all Remote Process Groups that are children or
     * descendants of this ProcessGroup. This performs a recursive search of all
     * descendant ProcessGroups
     *
     * @return
     */
    List<RemoteProcessGroup> findAllRemoteProcessGroups();

    /**
     * Returns the Processor with the given ID, if it exists as a child or
     * descendant of this ProcessGroup. This performs a recursive search of all
     * descendant ProcessGroups
     *
     * @param id
     * @return
     */
    ProcessorNode findProcessor(String id);

    /**
     * Returns a List of all Processors that are children or descendants of this
     * ProcessGroup. This performs a recursive search of all descendant
     * ProcessGroups
     *
     * @return
     */
    List<ProcessorNode> findAllProcessors();

    /**
     * Returns a List of all Labels that are children or descendants of this
     * ProcessGroup. This performsn a recursive search of all descendant
     * ProcessGroups
     *
     * @return
     */
    List<Label> findAllLabels();

    /**
     * Returns the input port with the given ID, if it exists; otherwise returns
     * null. This performs a recursive search of all Input Ports and descendant
     * ProcessGroups
     *
     * @param id
     * @return
     */
    Port findInputPort(String id);

    /**
     * Returns the input port with the given name, if it exists; otherwise
     * returns null. ProcessGroups
     *
     * @param name
     * @return
     */
    Port getInputPortByName(String name);

    /**
     * Returns the output port with the given ID, if it exists; otherwise
     * returns null. This performs a recursive search of all Output Ports and
     * descendant ProcessGroups
     *
     * @param id
     * @return
     */
    Port findOutputPort(String id);

    /**
     * Returns the output port with the given name, if it exists; otherwise
     * returns null.
     *
     * @param name
     * @return
     */
    Port getOutputPortByName(String name);

    /**
     * Adds the given funnel to this ProcessGroup and starts it. While other components
     * do not automatically start, the funnel does by default because it is intended to be
     * more of a notional component that users are unable to explicitly start and stop.
     * However, there is an override available in {@link #addFunnel(Funnel, boolean)} because
     * we may need to avoid starting the funnel on restart until the flow is completely
     * initialized.
     *
     * @param funnel
     */
    void addFunnel(Funnel funnel);
    
    /**
     * Adds the given funnel to this ProcessGroup and optionally starts the funnel.
     * @param funnel
     * @param autoStart
     */
    void addFunnel(Funnel funnel, boolean autoStart);

    /**
     * Returns a Set of all Funnels that belong to this ProcessGroup
     *
     * @return
     */
    Set<Funnel> getFunnels();

    /**
     * Returns the funnel with the given identifier
     *
     * @param id
     * @return
     */
    Funnel getFunnel(String id);

    /**
     * Removes the given funnel from this ProcessGroup
     *
     * @param funnel
     *
     * @throws IllegalStateException if the funnel is not a member of this
     * ProcessGroup or has incoming or outgoing connections
     */
    void removeFunnel(Funnel funnel);

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
     * @param snippet
     *
     * @throws NullPointerException if argument is null
     * @throws IllegalStateException if any ID in the snippet refers to a
     * component that is not within this ProcessGroup
     */
    void remove(final Snippet snippet);

    /**
     * Returns the Connectable with the given ID, if it exists; otherwise
     * returns null. This performs a recursive search of all ProcessGroups'
     * input ports, output ports, funnels, processors, and remote process groups
     *
     * @param identifier
     * @return
     */
    Connectable findConnectable(String identifier);

    /**
     * Moves all of the components whose ID's are specified within the given
     * {@link Snippet} from this ProcessGroup into the given destination
     * ProcessGroup
     *
     * @param snippet
     * @param destination
     *
     * @throws NullPointerExcepiton if either argument is null
     * @throws IllegalStateException if any ID in the snippet refers to a
     * component that is not within this ProcessGroup
     */
    void move(final Snippet snippet, final ProcessGroup destination);

    void verifyCanDelete();

    void verifyCanStart();

    void verifyCanStop();

    /**
     * Ensures that deleting the given snippet is a valid operation at this
     * point in time, depending on the state of this ProcessGroup
     *
     * @param snippet
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
     * @param snippet
     * @param newProcessGroup
     *
     * @throws IllegalStateException if the move is not valid at this time
     */
    void verifyCanMove(Snippet snippet, ProcessGroup newProcessGroup);
}
