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
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.X509ExtendedKeyManager;

/**
 * Key Manager Factory that returns a single Key Manager provided at construction time, instead of loading
 * a Key Store. This bridges client libraries whose SSL configuration requires a KeyManagerFactory with
 * SSLContextProvider implementations, which expose an already-initialized X.509 Key Manager directly.
 */
public class PredefinedKeyManagerFactory extends KeyManagerFactory {

    private static final String ALGORITHM = "PredefinedKeyManager";

    public PredefinedKeyManagerFactory(final X509ExtendedKeyManager keyManager) {
        super(new PredefinedKeyManagerFactorySpi(keyManager), new PredefinedSecurityProvider(), ALGORITHM);
    }

    private static class PredefinedKeyManagerFactorySpi extends KeyManagerFactorySpi {

        private final KeyManager[] keyManagers;

        private PredefinedKeyManagerFactorySpi(final X509ExtendedKeyManager keyManager) {
            this.keyManagers = new KeyManager[]{keyManager};
        }

        @Override
        protected void engineInit(final KeyStore keyStore, final char[] password) {
        }

        @Override
        protected void engineInit(final ManagerFactoryParameters managerFactoryParameters) {
        }

        @Override
        protected KeyManager[] engineGetKeyManagers() {
            return keyManagers;
        }
    }
}
