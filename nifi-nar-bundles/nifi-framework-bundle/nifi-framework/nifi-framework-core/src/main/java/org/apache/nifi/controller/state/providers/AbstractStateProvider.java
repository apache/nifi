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

package org.apache.nifi.controller.state.providers;

import java.io.IOException;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;

public abstract class AbstractStateProvider extends AbstractConfigurableComponent implements StateProvider {
    private String identifier;

    private volatile boolean enabled;

    @Override
    public final void initialize(final StateProviderInitializationContext context) throws IOException {
        this.identifier = context.getIdentifier();
        init(context);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void enable() {
        enabled = true;
    }

    @Override
    public void disable() {
        enabled = false;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    public abstract void init(final StateProviderInitializationContext context) throws IOException;
}
