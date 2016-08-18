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
package org.apache.nifi.processor;

import java.io.File;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.NiFiProperties;

public class StandardProcessorInitializationContext implements ProcessorInitializationContext {

    private final String identifier;
    private final ComponentLog logger;
    private final ControllerServiceProvider serviceProvider;
    private final NodeTypeProvider nodeTypeProvider;
    private final NiFiProperties nifiProperties;

    public StandardProcessorInitializationContext(
            final String identifier, final ComponentLog componentLog,
            final ControllerServiceProvider serviceProvider, final NodeTypeProvider nodeTypeProvider,
            final NiFiProperties nifiProperties) {
        this.identifier = identifier;
        this.logger = componentLog;
        this.serviceProvider = serviceProvider;
        this.nodeTypeProvider = nodeTypeProvider;
        this.nifiProperties = nifiProperties;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return serviceProvider;
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return nodeTypeProvider;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return nifiProperties.getKerberosServicePrincipal();
    }

    @Override
    public File getKerberosServiceKeytab() {
        return nifiProperties.getKerberosServiceKeytabLocation() == null ? null : new File(nifiProperties.getKerberosServiceKeytabLocation());
    }

    @Override
    public File getKerberosConfigurationFile() {
        return nifiProperties.getKerberosConfigurationFile();
    }
}
