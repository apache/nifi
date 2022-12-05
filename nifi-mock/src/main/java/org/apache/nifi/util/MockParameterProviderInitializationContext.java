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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.kerberos.KerberosContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.ParameterProviderInitializationContext;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class MockParameterProviderInitializationContext extends MockControllerServiceLookup implements ParameterProviderInitializationContext, ControllerServiceLookup {

    private final String identifier;
    private final String name;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final ComponentLog logger;
    private final KerberosContext kerberosContext;

    public MockParameterProviderInitializationContext(final String identifier, final String name, final ComponentLog logger) {
        this(identifier, name, logger, null);
    }

    public MockParameterProviderInitializationContext(final String identifier, final String name, final ComponentLog logger, final KerberosContext kerberosContext) {
        this.identifier = identifier;
        this.name = name;
        this.logger = logger;
        this.kerberosContext = kerberosContext;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setProperty(final String propertyName, final String value) {
        setProperty(new PropertyDescriptor.Builder().name(propertyName).build(), value);
    }

    public void setProperty(final PropertyDescriptor propertyName, final String value) {
        this.properties.put(propertyName, value);
    }

    public void setProperties(final Map<PropertyDescriptor, String> properties) {
        this.properties.clear();
        this.properties.putAll(properties);
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return null;
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return kerberosContext != null ? kerberosContext.getKerberosServicePrincipal() : null;
    }

    @Override
    public File getKerberosServiceKeytab() {
        return kerberosContext != null ? kerberosContext.getKerberosServiceKeytab() : null;
    }

    @Override
    public File getKerberosConfigurationFile() {
        return kerberosContext != null ? kerberosContext.getKerberosConfigurationFile() : null;
    }
}
