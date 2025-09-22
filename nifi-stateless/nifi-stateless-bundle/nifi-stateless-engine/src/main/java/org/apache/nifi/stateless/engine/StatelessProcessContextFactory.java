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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.scheduling.ConnectableProcessContext;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.StandardProcessContext;

public class StatelessProcessContextFactory implements ProcessContextFactory {
    private static final NodeTypeProvider NODE_TYPE_PROVIDER = new StatelessNodeTypeProvider();
    private final ControllerServiceProvider controllerServiceProvider;
    private final StateManagerProvider stateManagerProvider;

    public StatelessProcessContextFactory(final ControllerServiceProvider controllerServiceProvider, final StateManagerProvider stateManagerProvider) {
        this.controllerServiceProvider = controllerServiceProvider;
        this.stateManagerProvider = stateManagerProvider;
    }

    @Override
    public ProcessContext createProcessContext(final Connectable connectable) {
        final Class<?> componentClass = (connectable instanceof ProcessorNode && ((ProcessorNode) connectable).getProcessor() != null)
            ? ((ProcessorNode) connectable).getProcessor().getClass()
            : null;
        final StateManager stateManager = stateManagerProvider.getStateManager(connectable.getIdentifier(), componentClass);

        if (connectable instanceof ProcessorNode) {
            final ProcessorNode processor = (ProcessorNode) connectable;
            return new StandardProcessContext(processor, controllerServiceProvider, stateManager, () -> false, NODE_TYPE_PROVIDER);
        }

        return new ConnectableProcessContext(connectable, stateManager);
    }
}
