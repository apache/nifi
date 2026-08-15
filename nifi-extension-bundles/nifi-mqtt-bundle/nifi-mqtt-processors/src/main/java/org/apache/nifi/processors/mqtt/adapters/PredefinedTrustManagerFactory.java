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
package org.apache.nifi.processors.mqtt.adapters;

import java.security.KeyStore;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509TrustManager;

/**
 * Trust Manager Factory that returns a single Trust Manager provided at construction time, instead of
 * loading a Key Store. This bridges client libraries whose SSL configuration requires a
 * TrustManagerFactory with SSLContextProvider implementations, which expose an already-initialized
 * X.509 Trust Manager directly.
 */
public class PredefinedTrustManagerFactory extends TrustManagerFactory {

    private static final String ALGORITHM = "PredefinedTrustManager";

    public PredefinedTrustManagerFactory(final X509TrustManager trustManager) {
        super(new PredefinedTrustManagerFactorySpi(trustManager), new PredefinedSecurityProvider(), ALGORITHM);
    }

    private static class PredefinedTrustManagerFactorySpi extends TrustManagerFactorySpi {

        private final TrustManager[] trustManagers;

        private PredefinedTrustManagerFactorySpi(final X509TrustManager trustManager) {
            this.trustManagers = new TrustManager[]{trustManager};
        }

        @Override
        protected void engineInit(final KeyStore keyStore) {
        }

        @Override
        protected void engineInit(final ManagerFactoryParameters managerFactoryParameters) {
        }

        @Override
        protected TrustManager[] engineGetTrustManagers() {
            return trustManagers;
        }
    }
}
