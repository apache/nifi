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
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultComponentScheduler extends AbstractComponentScheduler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultComponentScheduler.class);

    public DefaultComponentScheduler(final ControllerServiceProvider controllerServiceProvider, final VersionedComponentStateLookup stateLookup) {
        super(controllerServiceProvider, stateLookup);
    }

    @Override
    protected void startNow(final Connectable component) {
        if (ExecutionEngine.STATELESS == component.getProcessGroup().resolveExecutionEngine()) {
            logger.info("{} should be running but will not start it because its Process Group is configured to run Stateless", component);
            return;
        }

        switch (component.getConnectableType()) {
            case PROCESSOR -> {
                final ProcessorNode processorNode = (ProcessorNode) component;
                processorNode.getProcessGroup().startProcessor(processorNode, false);
            }
            case INPUT_PORT -> {
                final Port port = (Port) component;
                port.getProcessGroup().startInputPort(port);
            }
            case OUTPUT_PORT -> {
                final Port port = (Port) component;
                port.getProcessGroup().startOutputPort(port);
            }
            case REMOTE_INPUT_PORT, REMOTE_OUTPUT_PORT -> {
                final RemoteGroupPort port = (RemoteGroupPort) component;
                port.getRemoteProcessGroup().startTransmitting(port);
            }
        }
    }

    protected void startNow(final ReportingTaskNode reportingTask) {
        reportingTask.start();
    }

    protected void startNow(final ProcessGroup statelessGroup) {
        statelessGroup.startProcessing();
    }
}
