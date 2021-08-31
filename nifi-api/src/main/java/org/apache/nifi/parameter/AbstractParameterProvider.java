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

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractParameterProvider extends AbstractConfigurableComponent implements ParameterProvider {
    private String identifier;
    private String name;
    private ControllerServiceLookup serviceLookup;
    private ComponentLog logger;
    private NodeTypeProvider nodeTypeProvider;

    @Override
    public final void initialize(final ParameterProviderInitializationContext config) throws InitializationException {
        identifier = config.getIdentifier();
        logger = config.getLogger();
        name = config.getName();
        serviceLookup = config.getControllerServiceLookup();
        nodeTypeProvider = config.getNodeTypeProvider();
        verifyInterfaces();
        init(config);
    }

    private void verifyInterfaces() {
        if (!(this instanceof SensitiveParameterProvider) && !(this instanceof NonSensitiveParameterProvider)) {
            throw new IllegalStateException(String.format("Parameter Provider [%s] must be either a SensitiveParameterProvider or a NonSensitiveParameterProvider, but is neither",
                    name));
        }
        if ((this instanceof SensitiveParameterProvider) && (this instanceof NonSensitiveParameterProvider)) {
            throw new IllegalStateException(String.format("Parameter Provider [%s] must be either a SensitiveParameterProvider or a NonSensitiveParameterProvider, but is both",
                    name));
        }
    }

    /**
     * @return the identifier of this Parameter Provider
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Calls {@link AbstractParameterProvider#fetchParameterList(ConfigurationContext)} and returns the parameters
     * as a map.
     * @param context The configuration context
     * @return A map from <code>ParameterDescriptor</code> to <code>Parameter</code>
     */
    @Override
    public final Map<ParameterDescriptor, Parameter> fetchParameters(final ConfigurationContext context) {
        this.name = context.getName();

        final List<Parameter> parameters = Objects.requireNonNull(this.fetchParameterList(context), "Fetched parameter list may not be null");
        final Map<ParameterDescriptor, Parameter> parameterMap = new LinkedHashMap<>(parameters.size());
        for(final Parameter parameter : parameters) {
            Objects.requireNonNull(parameter, "Fetched parameters may not be null");
            final ParameterDescriptor descriptor = Objects.requireNonNull(parameter.getDescriptor(),
                    "Fetched parameter descriptors may not be null");

            parameterMap.put(descriptor, parameter);
        }
        return Collections.unmodifiableMap(parameterMap);
    }

    /**
     * @return the {@link ControllerServiceLookup} that was passed to the
     * {@link #initialize(ParameterProviderInitializationContext)} method
     */
    protected final ControllerServiceLookup getControllerServiceLookup() {
        return serviceLookup;
    }

    /**
     * @return the {@link NodeTypeProvider} that was passed to the
     * {@link #initialize(ParameterProviderInitializationContext)} method
     */
    protected final NodeTypeProvider getNodeTypeProvider() {
        return nodeTypeProvider;
    }

    /**
     * @return the name of this Parameter Provider
     */
    protected String getName() {
        return name;
    }

    /**
     * @return the logger that has been provided to the component by the
     * framework in its initialize method
     */
    protected ComponentLog getLogger() {
        return logger;
    }

    /**
     * Provides a mechanism by which subclasses can perform initialization of
     * the Parameter Provider before its parameters are fetched
     *
     * @param config context
     * @throws InitializationException if failure to init
     */
    protected void init(final ParameterProviderInitializationContext config) throws InitializationException {
    }

    /**
     * Fetch the list of parameters.  The sensitivity of the <code>ParameterDescriptor</code> in each
     * <code>Parameter</code> must match the sensitivity of the <code>ParameterProvider</code> interface, whether
     * <code>SensitiveParameterProvider</code> or <code>NonSensitiveParameterProvider</code>.
     * @param context The configuration context
     * @return A list of fetched <code>Parameter</code>s
     */
    protected abstract List<Parameter> fetchParameterList(final ConfigurationContext context);
}
