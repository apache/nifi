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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.dao.ConnectorManagedComponentLookup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Default {@link ConnectorManagedComponentLookup} implementation. Delegates each lookup to the package-private
 * {@code locate*(id, true)} helper on the corresponding {@code Standard*DAO}, which performs the same search the
 * standard DAO surface performs but skips the verification that would otherwise reject access to a component inside a
 * Connector-managed Process Group whose owning Connector is not in Troubleshooting mode.
 */
@Component
public class StandardConnectorManagedComponentLookup extends ComponentDAO implements ConnectorManagedComponentLookup {

    private FlowController flowController;
    private StandardProcessorDAO processorDAO;
    private StandardConnectionDAO connectionDAO;
    private StandardInputPortDAO inputPortDAO;
    private StandardOutputPortDAO outputPortDAO;
    private StandardLabelDAO labelDAO;
    private StandardFunnelDAO funnelDAO;
    private StandardRemoteProcessGroupDAO remoteProcessGroupDAO;
    private StandardControllerServiceDAO controllerServiceDAO;

    @Override
    public ProcessorNode getProcessor(final String id) {
        return processorDAO.locateProcessor(id, true);
    }

    @Override
    public Port getInputPort(final String id) {
        return inputPortDAO.locatePort(id, true);
    }

    @Override
    public Port getOutputPort(final String id) {
        return outputPortDAO.locatePort(id, true);
    }

    @Override
    public Connection getConnection(final String id) {
        return connectionDAO.locateConnection(id, true);
    }

    @Override
    public ProcessGroup getProcessGroup(final String id) {
        return locateProcessGroup(flowController, id, true);
    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup(final String id) {
        return remoteProcessGroupDAO.locateRemoteProcessGroup(id, true);
    }

    @Override
    public ControllerServiceNode getControllerService(final String id) {
        return controllerServiceDAO.locateControllerService(id, true);
    }

    @Override
    public Label getLabel(final String id) {
        return labelDAO.locateLabel(id, true);
    }

    @Override
    public Funnel getFunnel(final String id) {
        return funnelDAO.locateFunnel(id, true);
    }

    @Autowired
    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Autowired
    public void setProcessorDAO(final StandardProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    @Autowired
    public void setConnectionDAO(final StandardConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    @Autowired
    public void setInputPortDAO(final StandardInputPortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    @Autowired
    public void setOutputPortDAO(final StandardOutputPortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    @Autowired
    public void setLabelDAO(final StandardLabelDAO labelDAO) {
        this.labelDAO = labelDAO;
    }

    @Autowired
    public void setFunnelDAO(final StandardFunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    @Autowired
    public void setRemoteProcessGroupDAO(final StandardRemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    @Autowired
    public void setControllerServiceDAO(final StandardControllerServiceDAO controllerServiceDAO) {
        this.controllerServiceDAO = controllerServiceDAO;
    }
}
