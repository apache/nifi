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
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;

public abstract class AbstractParameterProvider extends AbstractConfigurableComponent implements ParameterProvider {
    private String identifier;
    private String name;
    private ComponentLog logger;
    private NodeTypeProvider nodeTypeProvider;

    @Override
    public final void initialize(final ParameterProviderInitializationContext config) throws InitializationException {
        identifier = config.getIdentifier();
        logger = config.getLogger();
        name = config.getName();
        nodeTypeProvider = config.getNodeTypeProvider();
        init(config);
    }

    /**
     * @return the identifier of this Parameter Provider
     */
    @Override
    public String getIdentifier() {
        return identifier;
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
}
