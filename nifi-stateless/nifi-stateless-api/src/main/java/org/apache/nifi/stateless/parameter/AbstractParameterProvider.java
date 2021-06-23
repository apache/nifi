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

package org.apache.nifi.stateless.parameter;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.context.PropertyContext;

public abstract class AbstractParameterProvider extends AbstractConfigurableComponent implements ParameterProvider {
    private ParameterProviderInitializationContext context;

    @Override
    public final void initialize(final ParameterProviderInitializationContext context) {
        this.context = context;
        init(context);
    }

    @Override
    public final String getIdentifier() {
        return context == null ? "<Unknown ID>" : context.getIdentifier();
    }

    /**
     * Provides PropertyContext to subclasses
     */
    protected final PropertyContext getPropertyContext() {
        return context;
    }

    /**
     * An empty method that is intended for subclasses to optionally override in order to provide initialization
     * @param context the initialization context
     */
    protected void init(ParameterProviderInitializationContext context) {

    }
}
