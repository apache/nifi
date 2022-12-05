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
package org.apache.nifi.registry.flow;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.logging.ComponentLog;

import javax.net.ssl.SSLContext;
import java.util.Optional;

public abstract class AbstractFlowRegistryClient extends AbstractConfigurableComponent implements FlowRegistryClient {

    private volatile String identifier;
    private volatile Optional<SSLContext> systemSslContext;
    private volatile ComponentLog logger;

    @Override
    public void initialize(final FlowRegistryClientInitializationContext context) {
        this.identifier = context.getIdentifier();
        this.logger = context.getLogger();
        this.systemSslContext = context.getSystemSslContext();
    }

    @Override
    public final String getIdentifier() {
        return identifier;
    }

    protected final ComponentLog getLogger() {
        return logger;
    }

    protected final Optional<SSLContext> getSystemSslContext() {
        return systemSslContext;
    }
}
