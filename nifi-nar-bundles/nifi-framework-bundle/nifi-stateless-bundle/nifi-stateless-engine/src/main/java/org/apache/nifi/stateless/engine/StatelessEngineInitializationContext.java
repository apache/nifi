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

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;

public class StatelessEngineInitializationContext {
    private final ControllerServiceProvider controllerServiceProvider;
    private final FlowManager flowManager;
    private final ProcessContextFactory processContextFactory;
    private final RepositoryContextFactory repositoryContextFactory;

    public StatelessEngineInitializationContext(final ControllerServiceProvider controllerServiceProvider, final FlowManager flowManager, final ProcessContextFactory processContextFactory,
                                                final RepositoryContextFactory repositoryContextFactory) {
        this.controllerServiceProvider = controllerServiceProvider;
        this.flowManager = flowManager;
        this.processContextFactory = processContextFactory;
        this.repositoryContextFactory = repositoryContextFactory;
    }

    public ControllerServiceProvider getControllerServiceProvider() {
        return controllerServiceProvider;
    }

    public FlowManager getFlowManager() {
        return flowManager;
    }

    public ProcessContextFactory getProcessContextFactory() {
        return processContextFactory;
    }

    public RepositoryContextFactory getRepositoryContextFactory() {
        return repositoryContextFactory;
    }
}
