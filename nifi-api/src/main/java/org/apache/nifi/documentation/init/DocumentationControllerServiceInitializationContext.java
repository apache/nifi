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
package org.apache.nifi.documentation.init;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.logging.ComponentLog;

import java.io.File;
import java.util.UUID;

public class DocumentationControllerServiceInitializationContext implements ControllerServiceInitializationContext {
    private final String id = UUID.randomUUID().toString();
    private final ControllerServiceLookup serviceLookup = new EmptyControllerServiceLookup();
    private final ComponentLog componentLog = new NopComponentLog();

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return serviceLookup;
    }

    @Override
    public ComponentLog getLogger() {
        return componentLog;
    }

    @Override
    public StateManager getStateManager() {
        return new NopStateManager();
    }

    @Override
    public String getKerberosServicePrincipal() {
        return null;
    }

    @Override
    public File getKerberosServiceKeytab() {
        return null;
    }

    @Override
    public File getKerberosConfigurationFile() {
        return null;
    }
}
