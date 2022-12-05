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
package org.apache.nifi.parameter;

import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.logging.ComponentLog;

import java.io.File;

public class StandardParameterProviderInitializationContext implements ParameterProviderInitializationContext {

    private final String id;
    private final String name;
    private final ComponentLog logger;
    private final KerberosConfig kerberosConfig;
    private final NodeTypeProvider nodeTypeProvider;

    public StandardParameterProviderInitializationContext(final String id, final String name, final ComponentLog logger, final KerberosConfig kerberosConfig,
                                                  final NodeTypeProvider nodeTypeProvider) {
        this.id = id;
        this.name = name;
        this.logger = logger;
        this.kerberosConfig = kerberosConfig;
        this.nodeTypeProvider = nodeTypeProvider;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return kerberosConfig == null ? null : kerberosConfig.getPrincipal();
    }

    @Override
    public File getKerberosServiceKeytab() {
        return kerberosConfig == null ? null : kerberosConfig.getKeytabLocation();
    }

    @Override
    public File getKerberosConfigurationFile() {
        return kerberosConfig == null ? null : kerberosConfig.getConfigFile();
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return nodeTypeProvider;
    }
}
