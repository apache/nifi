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
package org.apache.nifi.cdc.mysql.processors.ssl;

import java.security.Provider;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;

/**
 * Java Security Provider that exposes a single pre-initialized {@link SSLContext} under the standard TLS algorithm.
 * MySQL Connector/J can be directed to this provider using the {@code sslContextProvider} connection property, which
 * allows an in-memory SSLContext to be used for JDBC connections without configuring keystore or truststore files.
 * Instances are expected to be registered under a unique name for the duration of a connection attempt and removed
 * afterward to avoid retaining references in the JVM-wide security provider registry.
 */
public final class DelegatingSSLContextProvider extends Provider {

    private static final String PROVIDER_VERSION = "1.0";

    private static final String SERVICE_TYPE = "SSLContext";

    private static final String TLS_ALGORITHM = "TLS";

    public DelegatingSSLContextProvider(final String name, final SSLContext sslContext) {
        super(name, PROVIDER_VERSION, "Delegates the TLS SSLContext to a pre-initialized instance");
        putService(new DelegatingService(this, sslContext));
    }

    private static final class DelegatingService extends Service {

        private final SSLContext sslContext;

        private DelegatingService(final Provider provider, final SSLContext sslContext) {
            super(provider, SERVICE_TYPE, TLS_ALGORITHM, DelegatingSSLContextSpi.class.getName(), List.of(), Map.of());
            this.sslContext = sslContext;
        }

        @Override
        public Object newInstance(final Object constructorParameter) {
            return new DelegatingSSLContextSpi(sslContext);
        }
    }
}
