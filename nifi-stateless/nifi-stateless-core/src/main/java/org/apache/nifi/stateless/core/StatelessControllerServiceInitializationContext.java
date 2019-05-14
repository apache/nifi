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
package org.apache.nifi.stateless.core;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.logging.ComponentLog;

import java.io.File;

public class StatelessControllerServiceInitializationContext implements ControllerServiceInitializationContext {
    private final ComponentLog logger;
    private final String processorId;
    private final ControllerServiceLookup controllerServiceLookup;
    private final StateManager stateManager;

    public StatelessControllerServiceInitializationContext(final String id, final ControllerService controllerService, final ControllerServiceLookup serviceLookup, final StateManager stateManager) {
        processorId = id;
        logger = new SLF4JComponentLog(controllerService);
        controllerServiceLookup = serviceLookup;
        this.stateManager = stateManager;
    }

    public String getIdentifier() {
        return processorId;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public StateManager getStateManager() {
        return stateManager;
    }

    public ControllerServiceLookup getControllerServiceLookup() {
        return controllerServiceLookup;
    }

    public NodeTypeProvider getNodeTypeProvider() {
        return new NodeTypeProvider() {
            public boolean isClustered() {
                return false;
            }

            public boolean isPrimary() {
                return false;
            }
        };
    }

    public String getKerberosServicePrincipal() {
        return null; //this needs to be wired in.
    }

    public File getKerberosServiceKeytab() {
        return null; //this needs to be wired in.
    }

    public File getKerberosConfigurationFile() {
        return null; //this needs to be wired in.
    }}
