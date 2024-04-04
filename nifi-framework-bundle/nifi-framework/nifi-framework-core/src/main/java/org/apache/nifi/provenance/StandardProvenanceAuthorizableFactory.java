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
package org.apache.nifi.provenance;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.ProvenanceDataAuthorizable;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.ResourceNotFoundException;

public class StandardProvenanceAuthorizableFactory implements ProvenanceAuthorizableFactory {
    private final FlowController flowController;

    public StandardProvenanceAuthorizableFactory(final FlowController flowController) {
        this.flowController = flowController;
    }


    @Override
    public Authorizable createLocalDataAuthorizable(final String componentId) {
        final FlowManager flowManager = flowController.getFlowManager();
        final String rootGroupId = flowManager.getRootGroupId();

        // Provenance Events are generated only by connectable components, with the exception of DOWNLOAD events,
        // which have the root process group's identifier assigned as the component ID, and DROP events, which
        // could have the connection identifier assigned as the component ID. So, we check if the component ID
        // is set to the root group and otherwise assume that the ID is that of a connectable or connection.
        final DataAuthorizable authorizable;
        if (rootGroupId.equals(componentId)) {
            authorizable = new DataAuthorizable(flowManager.getRootGroup());
        } else {
            // check if the component is a connectable, this should be the case most often
            final Connectable connectable = flowManager.findConnectable(componentId);
            if (connectable == null) {
                // if the component id is not a connectable then consider a connection
                final Connection connection = flowManager.getRootGroup().findConnection(componentId);

                if (connection == null) {
                    throw new ResourceNotFoundException("The component that generated this event is no longer part of the data flow.");
                } else {
                    // authorizable for connection data is associated with the source connectable
                    authorizable = new DataAuthorizable(connection.getSource());
                }
            } else {
                authorizable = new DataAuthorizable(connectable);
            }
        }

        return authorizable;
    }



    @Override
    public Authorizable createRemoteDataAuthorizable(String remoteGroupPortId) {
        final DataAuthorizable authorizable;

        final RemoteGroupPort remoteGroupPort = flowController.getFlowManager().getRootGroup().findRemoteGroupPort(remoteGroupPortId);
        if (remoteGroupPort == null) {
            throw new ResourceNotFoundException("The component that generated this event is no longer part of the data flow.");
        } else {
            // authorizable for remote group ports should be the remote process group
            authorizable = new DataAuthorizable(remoteGroupPort.getRemoteProcessGroup());
        }

        return authorizable;
    }

    @Override
    public Authorizable createProvenanceDataAuthorizable(String componentId) {
        final FlowManager flowManager = flowController.getFlowManager();
        final String rootGroupId = flowManager.getRootGroupId();

        // Provenance Events are generated only by connectable components, with the exception of DOWNLOAD events,
        // which have the root process group's identifier assigned as the component ID, and DROP events, which
        // could have the connection identifier assigned as the component ID. So, we check if the component ID
        // is set to the root group and otherwise assume that the ID is that of a connectable or connection.
        final ProvenanceDataAuthorizable authorizable;
        if (rootGroupId.equals(componentId)) {
            authorizable = new ProvenanceDataAuthorizable(flowManager.getRootGroup());
        } else {
            // check if the component is a connectable, this should be the case most often
            final Connectable connectable = flowManager.findConnectable(componentId);
            if (connectable == null) {
                // if the component id is not a connectable then consider a connection
                final Connection connection = flowManager.getRootGroup().findConnection(componentId);

                if (connection == null) {
                    throw new ResourceNotFoundException("The component that generated this event is no longer part of the data flow.");
                } else {
                    // authorizable for connection data is associated with the source connectable
                    authorizable = new ProvenanceDataAuthorizable(connection.getSource());
                }
            } else {
                authorizable = new ProvenanceDataAuthorizable(connectable);
            }
        }

        return authorizable;
    }
}
