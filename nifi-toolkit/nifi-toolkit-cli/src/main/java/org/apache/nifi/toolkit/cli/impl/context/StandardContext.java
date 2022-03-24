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
package org.apache.nifi.toolkit.cli.impl.context;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;

import java.io.PrintStream;

/**
 * Context for the CLI which will be passed to each command.
 */
public class StandardContext implements Context {

    private final ClientFactory<NiFiClient> niFiClientFactory;
    private final ClientFactory<NiFiRegistryClient> niFiRegistryClientFactory;
    private final Session session;
    private final PrintStream output;
    private final boolean isInteractive;

    private StandardContext(final Builder builder) {
        this.niFiClientFactory = builder.niFiClientFactory;
        this.niFiRegistryClientFactory = builder.niFiRegistryClientFactory;
        this.session = builder.session;
        this.output = builder.output;
        this.isInteractive = builder.isInteractive;

        Validate.notNull(this.niFiClientFactory);
        Validate.notNull(this.niFiRegistryClientFactory);
        Validate.notNull(this.session);
        Validate.notNull(this.output);
    }

    @Override
    public ClientFactory<NiFiClient> getNiFiClientFactory() {
        return niFiClientFactory;
    }

    @Override
    public ClientFactory<NiFiRegistryClient> getNiFiRegistryClientFactory() {
        return niFiRegistryClientFactory;
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public PrintStream getOutput() {
        return output;
    }

    @Override
    public boolean isInteractive() {
        return isInteractive;
    }


    public static class Builder {
        private ClientFactory<NiFiClient> niFiClientFactory;
        private ClientFactory<NiFiRegistryClient> niFiRegistryClientFactory;
        private Session session;
        private PrintStream output;
        private boolean isInteractive;

        public Builder nifiClientFactory(final ClientFactory<NiFiClient> niFiClientFactory) {
            this.niFiClientFactory = niFiClientFactory;
            return this;
        }

        public Builder nifiRegistryClientFactory(final ClientFactory<NiFiRegistryClient> niFiRegistryClientFactory) {
            this.niFiRegistryClientFactory = niFiRegistryClientFactory;
            return this;
        }

        public Builder session(final Session session) {
            this.session = session;
            return this;
        }

        public Builder output(final PrintStream output) {
            this.output = output;
            return this;
        }

        public Builder interactive(final boolean isInteractive) {
            this.isInteractive = isInteractive;
            return this;
        }

        public StandardContext build() {
            return new StandardContext(this);
        }

    }
}
