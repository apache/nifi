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

package org.apache.nifi.controller.service.mock;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockProcessGroup implements ProcessGroup {
    private Map<String, ControllerServiceNode> serviceMap = new HashMap<>();

    @Override
    public Authorizable getParentAuthorizable() {
        return null;
    }

    @Override
    public Resource getResource() {
        return null;
    }

    @Override
    public ProcessGroup getParent() {
        return null;
    }

    @Override
    public void setParent(ProcessGroup group) {

    }

    @Override
    public String getIdentifier() {
        return "unit test group id";
    }

    @Override
    public String getName() {
        return "unit test group";
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public void setPosition(Position position) {

    }

    @Override
    public Position getPosition() {
        return null;
    }

    @Override
    public String getComments() {
        return null;
    }

    @Override
    public void setComments(String comments) {

    }

    @Override
    public ProcessGroupCounts getCounts() {
        return null;
    }

    @Override
    public void startProcessing() {

    }

    @Override
    public void stopProcessing() {

    }

    @Override
    public void enableProcessor(ProcessorNode processor) {

    }

    @Override
    public void enableInputPort(Port port) {

    }

    @Override
    public void enableOutputPort(Port port) {

    }

    @Override
    public void startProcessor(ProcessorNode processor) {

    }

    @Override
    public void startInputPort(Port port) {

    }

    @Override
    public void startOutputPort(Port port) {

    }

    @Override
    public void startFunnel(Funnel funnel) {

    }

    @Override
    public void stopProcessor(ProcessorNode processor) {

    }

    @Override
    public void stopInputPort(Port port) {

    }

    @Override
    public void stopOutputPort(Port port) {

    }

    @Override
    public void disableProcessor(ProcessorNode processor) {

    }

    @Override
    public void disableInputPort(Port port) {

    }

    @Override
    public void disableOutputPort(Port port) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isRootGroup() {
        return false;
    }

    @Override
    public void addInputPort(Port port) {

    }

    @Override
    public void removeInputPort(Port port) {

    }

    @Override
    public Set<Port> getInputPorts() {
        return null;
    }

    @Override
    public Port getInputPort(String id) {
        return null;
    }

    @Override
    public void addOutputPort(Port port) {

    }

    @Override
    public void removeOutputPort(Port port) {

    }

    @Override
    public Port getOutputPort(String id) {
        return null;
    }

    @Override
    public Set<Port> getOutputPorts() {
        return null;
    }

    @Override
    public void addProcessGroup(ProcessGroup group) {

    }

    @Override
    public ProcessGroup getProcessGroup(String id) {
        return null;
    }

    @Override
    public Set<ProcessGroup> getProcessGroups() {
        return null;
    }

    @Override
    public void removeProcessGroup(ProcessGroup group) {

    }

    @Override
    public void addProcessor(ProcessorNode processor) {
        processor.setProcessGroup(this);
    }

    @Override
    public void removeProcessor(ProcessorNode processor) {

    }

    @Override
    public Set<ProcessorNode> getProcessors() {
        return null;
    }

    @Override
    public ProcessorNode getProcessor(String id) {
        return null;
    }

    @Override
    public Set<Positionable> findAllPositionables() {
        return null;
    }

    @Override
    public Connectable getConnectable(String id) {
        return null;
    }

    @Override
    public void addConnection(Connection connection) {

    }

    @Override
    public void removeConnection(Connection connection) {

    }

    @Override
    public void inheritConnection(Connection connection) {

    }

    @Override
    public Connection getConnection(String id) {
        return null;
    }

    @Override
    public Set<Connection> getConnections() {
        return null;
    }

    @Override
    public Connection findConnection(String id) {
        return null;
    }

    @Override
    public List<Connection> findAllConnections() {
        return null;
    }

    @Override
    public Funnel findFunnel(String id) {
        return null;
    }

    @Override
    public ControllerServiceNode findControllerService(String id) {
        return serviceMap.get(id);
    }

    @Override
    public Set<ControllerServiceNode> findAllControllerServices() {
        return new HashSet<>(serviceMap.values());
    }

    @Override
    public void addRemoteProcessGroup(RemoteProcessGroup remoteGroup) {

    }

    @Override
    public void removeRemoteProcessGroup(RemoteProcessGroup remoteGroup) {

    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup(String id) {
        return null;
    }

    @Override
    public Set<RemoteProcessGroup> getRemoteProcessGroups() {
        return null;
    }

    @Override
    public void addLabel(Label label) {

    }

    @Override
    public void removeLabel(Label label) {

    }

    @Override
    public Set<Label> getLabels() {
        return null;
    }

    @Override
    public Label getLabel(String id) {
        return null;
    }

    @Override
    public ProcessGroup findProcessGroup(String id) {
        return null;
    }

    @Override
    public List<ProcessGroup> findAllProcessGroups() {
        return null;
    }

    @Override
    public RemoteProcessGroup findRemoteProcessGroup(String id) {
        return null;
    }

    @Override
    public List<RemoteProcessGroup> findAllRemoteProcessGroups() {
        return null;
    }

    @Override
    public ProcessorNode findProcessor(String id) {
        return null;
    }

    @Override
    public List<ProcessorNode> findAllProcessors() {
        return null;
    }

    @Override
    public Label findLabel(String id) {
        return null;
    }

    @Override
    public List<Label> findAllLabels() {
        return null;
    }

    @Override
    public Port findInputPort(String id) {
        return null;
    }

    @Override
    public List<Port> findAllInputPorts() {
        return null;
    }

    @Override
    public Port getInputPortByName(String name) {
        return null;
    }

    @Override
    public Port findOutputPort(String id) {
        return null;
    }

    @Override
    public List<Port> findAllOutputPorts() {
        return null;
    }

    @Override
    public Port getOutputPortByName(String name) {
        return null;
    }

    @Override
    public void addFunnel(Funnel funnel) {

    }

    @Override
    public void addFunnel(Funnel funnel, boolean autoStart) {

    }

    @Override
    public Set<Funnel> getFunnels() {
        return null;
    }

    @Override
    public Funnel getFunnel(String id) {
        return null;
    }

    @Override
    public void removeFunnel(Funnel funnel) {

    }

    @Override
    public void addControllerService(ControllerServiceNode service) {
        serviceMap.put(service.getIdentifier(), service);
        service.setProcessGroup(this);
    }

    @Override
    public ControllerServiceNode getControllerService(String id) {
        return serviceMap.get(id);
    }

    @Override
    public Set<ControllerServiceNode> getControllerServices(boolean recursive) {
        return new HashSet<>(serviceMap.values());
    }

    @Override
    public void removeControllerService(ControllerServiceNode service) {
        serviceMap.remove(service.getIdentifier());
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void remove(Snippet snippet) {

    }

    @Override
    public Connectable findConnectable(String identifier) {
        return null;
    }

    @Override
    public void move(Snippet snippet, ProcessGroup destination) {

    }

    @Override
    public void verifyCanDelete() {

    }

    @Override
    public void verifyCanDelete(boolean ignorePortConnections) {

    }

    @Override
    public void verifyCanStart() {

    }

    @Override
    public void verifyCanStop() {

    }

    @Override
    public void verifyCanDelete(Snippet snippet) {

    }

    @Override
    public void verifyCanMove(Snippet snippet, ProcessGroup newProcessGroup) {

    }

    @Override
    public void addTemplate(Template template) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeTemplate(Template template) {
    }

    @Override
    public Template getTemplate(String id) {
        return null;
    }

    @Override
    public Template findTemplate(String id) {
        return null;
    }

    @Override
    public Set<Template> getTemplates() {
        return null;
    }

    @Override
    public Set<Template> findAllTemplates() {
        return null;
    }

    @Override
    public void verifyCanStart(Connectable connectable) {
    }

    @Override
    public void verifyCanStop(Connectable connectable) {
    }
}
