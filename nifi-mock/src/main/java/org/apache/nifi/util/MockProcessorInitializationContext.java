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
package org.apache.nifi.util;

import java.io.File;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;

public class MockProcessorInitializationContext implements ProcessorInitializationContext, ControllerServiceLookup {

    private final MockComponentLog logger;
    private final String processorId;
    private final MockProcessContext context;

    public MockProcessorInitializationContext(final Processor processor, final MockProcessContext context) {
        processorId = UUID.randomUUID().toString();
        logger = new MockComponentLog(processorId, processor);
        this.context = context;
    }

    @Override
    public String getIdentifier() {
        return processorId;
    }

    @Override
    public MockComponentLog getLogger() {
        return logger;
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        return context.getControllerServiceIdentifiers(serviceType);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        return context.getControllerService(identifier);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        return context.getControllerServiceName(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        return context.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return context.isControllerServiceEnabled(service);
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return context.isControllerServiceEnabling(serviceIdentifier);
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return context;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return null; //this needs to be wired in.
    }

    @Override
    public File getKerberosServiceKeytab() {
        return null; //this needs to be wired in.
    }

    @Override
    public File getKerberosConfigurationFile() {
        return null; //this needs to be wired in.
    }
}
