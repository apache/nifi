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
package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleInitializationContext;
import org.apache.nifi.logging.ComponentLog;

import java.io.File;
import java.util.Set;

public class StandardFlowAnalysisInitializationContext implements FlowAnalysisRuleInitializationContext, ControllerServiceLookup {
    private final String id;
    private final ComponentLog logger;
    private final ControllerServiceProvider serviceProvider;
    private final KerberosConfig kerberosConfig;
    private final NodeTypeProvider nodeTypeProvider;

    public StandardFlowAnalysisInitializationContext(
            String id,
            ComponentLog logger,
            ControllerServiceProvider serviceProvider,
            KerberosConfig kerberosConfig,
            NodeTypeProvider nodeTypeProvider
    ) {
        this.id = id;
        this.serviceProvider = serviceProvider;
        this.logger = logger;
        this.kerberosConfig = kerberosConfig;
        this.nodeTypeProvider = nodeTypeProvider;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        return serviceProvider.getControllerServiceIdentifiers(serviceType, null);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        return serviceProvider.getControllerService(identifier);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return serviceProvider.isControllerServiceEnabled(service);
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        return serviceProvider.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return serviceProvider.isControllerServiceEnabling(serviceIdentifier);
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        return serviceProvider.getControllerServiceName(serviceIdentifier);
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return kerberosConfig.getPrincipal();
    }

    @Override
    public File getKerberosServiceKeytab() {
        return kerberosConfig.getKeytabLocation();
    }

    @Override
    public File getKerberosConfigurationFile() {
        return kerberosConfig.getConfigFile();
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return nodeTypeProvider;
    }
}
