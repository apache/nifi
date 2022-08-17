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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.remote.RemoteGroupPort;

import java.util.Collection;

public class DefaultComponentScheduler extends AbstractComponentScheduler {

    public DefaultComponentScheduler(final ControllerServiceProvider controllerServiceProvider, final VersionedComponentStateLookup stateLookup) {
        super(controllerServiceProvider, stateLookup);
    }

    @Override
    protected void startNow(final Connectable component) {
        switch (component.getConnectableType()) {
            case PROCESSOR: {
                final ProcessorNode processorNode = (ProcessorNode) component;
                processorNode.getProcessGroup().startProcessor(processorNode, false);
                break;
            }
            case INPUT_PORT: {
                final Port port = (Port) component;
                port.getProcessGroup().startInputPort(port);
                break;
            }
            case OUTPUT_PORT: {
                final Port port = (Port) component;
                port.getProcessGroup().startOutputPort(port);
                break;
            }
            case REMOTE_INPUT_PORT:
            case REMOTE_OUTPUT_PORT: {
                final RemoteGroupPort port = (RemoteGroupPort) component;
                port.getRemoteProcessGroup().startTransmitting(port);
            }
        }
    }

    @Override
    protected void enableNow(final Collection<ControllerServiceNode> controllerServices) {
        getControllerServiceProvider().enableControllerServices(controllerServices);
    }

    protected void startNow(final ReportingTaskNode reportingTask) {
        reportingTask.start();
    }
}
