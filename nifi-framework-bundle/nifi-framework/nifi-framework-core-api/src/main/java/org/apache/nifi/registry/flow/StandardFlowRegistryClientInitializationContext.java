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

import org.apache.nifi.logging.ComponentLog;

import javax.net.ssl.SSLContext;
import java.util.Optional;

public class StandardFlowRegistryClientInitializationContext implements FlowRegistryClientInitializationContext {

    private final String identifier;
    private final ComponentLog logger;
    private final Optional<SSLContext> systemSslContext;

    public StandardFlowRegistryClientInitializationContext(
            final String identifier,
            final ComponentLog logger,
            final SSLContext systemSslContext) {
        this.identifier = identifier;
        this.logger = logger;
        this.systemSslContext = Optional.ofNullable(systemSslContext);
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
    public Optional<SSLContext> getSystemSslContext() {
        return systemSslContext;
    }
}
