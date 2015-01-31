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
package org.apache.nifi.controller;

import java.util.Map;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.annotation.OnConfigured;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.reporting.InitializationException;

public abstract class AbstractControllerService extends AbstractConfigurableComponent implements ControllerService {

    private String identifier;
    private ControllerServiceLookup serviceLookup;
    private volatile ConfigurationContext configContext;

    @Override
    public final void initialize(final ControllerServiceInitializationContext context) throws InitializationException {
        this.identifier = context.getIdentifier();
        serviceLookup = context.getControllerServiceLookup();
        init(context);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @OnConfigured
    public void onConfigurationChange(final ConfigurationContext context) {
        this.configContext = context;
    }

    /**
     * Returns the currently configured value for the given
     * {@link PropertyDescriptor}
     *
     * @param descriptor
     * @return
     */
    protected final PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return configContext.getProperty(descriptor);
    }

    /**
     * Returns an unmodifiable map of all configured properties for this
     * {@link ControllerService}
     *
     * @return
     */
    protected final Map<PropertyDescriptor, String> getProperties() {
        return configContext.getProperties();
    }

    /**
     * Returns the {@link ControllerServiceLookup} that was passed to the
     * {@link #init(ProcessorInitializationContext)} method
     *
     * @return
     */
    protected final ControllerServiceLookup getControllerServiceLookup() {
        return serviceLookup;
    }

    /**
     * Provides a mechanism by which subclasses can perform initialization of
     * the Reporting Task before it is scheduled to be run
     *
     * @param config
     * @throws InitializationException
     */
    protected void init(final ControllerServiceInitializationContext config) throws InitializationException {
    }
}
