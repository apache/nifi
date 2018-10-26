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
package org.apache.nifi.controller.flow;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;

import java.net.URL;
import java.util.Collection;
import java.util.Set;

public interface FlowManager {
    String ROOT_GROUP_ID_ALIAS = "root";
    String DEFAULT_ROOT_GROUP_NAME = "NiFi Flow";

    /**
     * Creates a Port to use as an Input Port for receiving data via Site-to-Site communications
     *
     * @param id port id
     * @param name port name
     * @return new port
     * @throws NullPointerException if the ID or name is not unique
     * @throws IllegalStateException if a Port already exists with the same id.
     */
    Port createRemoteInputPort(String id, String name);

    /**
     * Creates a Port to use as an Output Port for transferring data via Site-to-Site communications
     *
     * @param id port id
     * @param name port name
     * @return new port
     * @throws NullPointerException if the ID or name is not unique
     * @throws IllegalStateException if a Port already exists with the same id.
     */
    Port createRemoteOutputPort(String id, String name);

    /**
     * Creates a new Remote Process Group with the given ID that points to the given URI
     *
     * @param id Remote Process Group ID
     * @param uris group uris, multiple url can be specified in comma-separated format
     * @return new remote process group
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if any of the <code>uri</code>s is not a valid URI.
     */
    RemoteProcessGroup createRemoteProcessGroup(String id, String uris);

    /**
     * @return the ProcessGroup that is currently assigned as the Root Group
     */
    ProcessGroup getRootGroup();

    String getRootGroupId();

    /**
     * Creates an instance of the given snippet and adds the components to the given group
     *
     * @param group group
     * @param dto dto
     *
     * @throws NullPointerException if either argument is null
     * @throws IllegalStateException if the snippet is not valid because a
     * component in the snippet has an ID that is not unique to this flow, or
     * because it shares an Input Port or Output Port at the root level whose
     * name already exists in the given ProcessGroup, or because the Template
     * contains a Processor or a Prioritizer whose class is not valid within
     * this instance of NiFi.
     * @throws ProcessorInstantiationException if unable to instantiate a
     * processor
     */
    void instantiateSnippet(ProcessGroup group, FlowSnippetDTO dto) throws ProcessorInstantiationException;

    /**
     * Indicates whether or not the two ID's point to the same ProcessGroup. If
     * either id is null, will return <code>false</code>.
     *
     * @param id1 group id
     * @param id2 other group id
     * @return true if same
     */
    boolean areGroupsSame(String id1, String id2);

    /**
     * Creates a new instance of the FlowFilePrioritizer with the given type
     * @param type the type of the prioritizer (fully qualified class name)
     * @return the newly created FlowFile Prioritizer
     */
    FlowFilePrioritizer createPrioritizer(String type) throws InstantiationException, IllegalAccessException, ClassNotFoundException;

    /**
     * Returns the ProcessGroup with the given ID, or null if no group exists with the given ID.
     * @param id id of the group
     * @return the ProcessGroup with the given ID or null if none can be found
     */
    ProcessGroup getGroup(String id);

    void onProcessGroupAdded(ProcessGroup group);

    void onProcessGroupRemoved(ProcessGroup group);


    /**
     * Finds the Connectable with the given ID, or null if no such Connectable exists
     * @param id the ID of the Connectable
     * @return the Connectable with the given ID, or null if no such Connectable exists
     */
    Connectable findConnectable(String id);

    /**
     * Returns the ProcessorNode with the given ID
     * @param id the ID of the Processor
     * @return the ProcessorNode with the given ID or null if no such Processor exists
     */
    ProcessorNode getProcessorNode(String id);

    void onProcessorAdded(ProcessorNode processor);

    void onProcessorRemoved(ProcessorNode processor);


    /**
     * <p>
     * Creates a new ProcessorNode with the given type and identifier and
     * initializes it invoking the methods annotated with {@link org.apache.nifi.annotation.lifecycle.OnAdded}.
     * </p>
     *
     * @param type processor type
     * @param id processor id
     * @param coordinate the coordinate of the bundle for this processor
     * @return new processor
     * @throws NullPointerException if either arg is null
     */
    ProcessorNode createProcessor(String type, String id, BundleCoordinate coordinate);

    /**
     * <p>
     * Creates a new ProcessorNode with the given type and identifier and
     * optionally initializes it.
     * </p>
     *
     * @param type the fully qualified Processor class name
     * @param id the unique ID of the Processor
     * @param coordinate the bundle coordinate for this processor
     * @param firstTimeAdded whether or not this is the first time this
     * Processor is added to the graph. If {@code true}, will invoke methods
     * annotated with the {@link org.apache.nifi.annotation.lifecycle.OnAdded} annotation.
     * @return new processor node
     * @throws NullPointerException if either arg is null
     */
    ProcessorNode createProcessor(String type, String id, BundleCoordinate coordinate, boolean firstTimeAdded);

    /**
     * <p>
     * Creates a new ProcessorNode with the given type and identifier and
     * optionally initializes it.
     * </p>
     *
     * @param type the fully qualified Processor class name
     * @param id the unique ID of the Processor
     * @param coordinate the bundle coordinate for this processor
     * @param firstTimeAdded whether or not this is the first time this
     * Processor is added to the graph. If {@code true}, will invoke methods
     * annotated with the {@link org.apache.nifi.annotation.lifecycle.OnAdded} annotation.
     * @return new processor node
     * @throws NullPointerException if either arg is null
     */
    ProcessorNode createProcessor(String type, String id, BundleCoordinate coordinate, Set<URL> additionalUrls, boolean firstTimeAdded, boolean registerLogObserver);



    Label createLabel(String id, String text);

    Funnel createFunnel(String id);

    Port createLocalInputPort(String id, String name);

    Port createLocalOutputPort(String id, String name);

    ProcessGroup createProcessGroup(String id);



    void onConnectionAdded(Connection connection);

    void onConnectionRemoved(Connection connection);

    Connection getConnection(String id);

    Set<Connection> findAllConnections();

    /**
     * Creates a connection between two Connectable objects.
     *
     * @param id required ID of the connection
     * @param name the name of the connection, or <code>null</code> to leave the connection unnamed
     * @param source required source
     * @param destination required destination
     * @param relationshipNames required collection of relationship names
     * @return the created Connection
     *
     * @throws NullPointerException if the ID, source, destination, or set of relationships is null.
     * @throws IllegalArgumentException if <code>relationships</code> is an empty collection
     */
    Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination, final Collection<String> relationshipNames);



    void onInputPortAdded(Port inputPort);

    void onInputPortRemoved(Port inputPort);

    Port getInputPort(String id);



    void onOutputPortAdded(Port outputPort);

    void onOutputPortRemoved(Port outputPort);

    Port getOutputPort(String id);



    void onFunnelAdded(Funnel funnel);

    void onFunnelRemoved(Funnel funnel);

    Funnel getFunnel(String id);



    ReportingTaskNode createReportingTask(String type, BundleCoordinate bundleCoordinate);

    ReportingTaskNode createReportingTask(String type, BundleCoordinate bundleCoordinate, boolean firstTimeAdded);

    ReportingTaskNode createReportingTask(String type, String id, BundleCoordinate bundleCoordinate, boolean firstTimeAdded);

    ReportingTaskNode createReportingTask(String type, String id, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls, boolean firstTimeAdded, boolean register);

    ReportingTaskNode getReportingTaskNode(String taskId);

    void removeReportingTask(ReportingTaskNode reportingTask);

    Set<ReportingTaskNode> getAllReportingTasks();



    Set<ControllerServiceNode> getAllControllerServices();

    ControllerServiceNode getControllerServiceNode(String id);

    ControllerServiceNode createControllerService(String type, String id, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls, boolean firstTimeAdded,
                                                         boolean registerLogObserver);


    Set<ControllerServiceNode> getRootControllerServices();

    void addRootControllerService(ControllerServiceNode serviceNode);

    ControllerServiceNode getRootControllerService(String serviceIdentifier);

    void removeRootControllerService(final ControllerServiceNode service);
}
