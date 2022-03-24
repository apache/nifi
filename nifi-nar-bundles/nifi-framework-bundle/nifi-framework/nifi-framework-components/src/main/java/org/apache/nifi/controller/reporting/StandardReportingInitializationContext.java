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
package org.apache.nifi.controller.reporting;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StandardReportingInitializationContext implements ReportingInitializationContext, ControllerServiceLookup {

    private final String id;
    private final String name;
    private final String schedulingPeriod;
    private final SchedulingStrategy schedulingStrategy;
    private final ControllerServiceProvider serviceProvider;
    private final ComponentLog logger;
    private final KerberosConfig kerberosConfig;
    private final NodeTypeProvider nodeTypeProvider;

    public StandardReportingInitializationContext(final String id, final String name, final SchedulingStrategy schedulingStrategy, final String schedulingPeriod,
                                                  final ComponentLog logger, final ControllerServiceProvider serviceProvider, final KerberosConfig kerberosConfig,
                                                  final NodeTypeProvider nodeTypeProvider) {
        this.id = id;
        this.name = name;
        this.schedulingPeriod = schedulingPeriod;
        this.serviceProvider = serviceProvider;
        this.schedulingStrategy = schedulingStrategy;
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
    public long getSchedulingPeriod(final TimeUnit timeUnit) {
        if (schedulingStrategy == SchedulingStrategy.TIMER_DRIVEN) {
            return FormatUtils.getTimeDuration(schedulingPeriod, timeUnit);
        }
        return -1L;
    }

    @Override
    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return schedulingStrategy;
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
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
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
