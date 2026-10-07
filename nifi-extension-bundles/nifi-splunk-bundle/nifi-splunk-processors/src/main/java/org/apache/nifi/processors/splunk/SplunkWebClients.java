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
package org.apache.nifi.processors.splunk;

import com.splunk.Service;
import com.splunk.WebClientSplunkService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.ssl.TlsContext;

import java.net.http.HttpClient;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

final class SplunkWebClients {
    private static final String USERNAME_ARGUMENT = "username";

    private SplunkWebClients() {
    }

    static StandardWebClientService create(final SSLContextProvider sslContextProvider, final String hostname, final ComponentLog logger) {
        final X509TrustManager delegateTrustManager = sslContextProvider.createTrustManager();
        final X509ExtendedTrustManager trustManager = new ConfiguredHostTrustManager(delegateTrustManager, hostname, logger);
        final Optional<X509KeyManager> keyManager = sslContextProvider.createKeyManager().map(X509KeyManager.class::cast);
        final SSLContext sslContext = sslContextProvider.createContext();

        return getClient(sslContext, trustManager, keyManager);
    }

    static Service connect(final Map<String, Object> serviceArgs, final WebClientService webClientService) {
        final WebClientSplunkService service = new WebClientSplunkService(serviceArgs, webClientService);
        if (serviceArgs.containsKey(USERNAME_ARGUMENT)) {
            service.login();
        }

        return service;
    }

    private static StandardWebClientService getClient(SSLContext sslContext, X509ExtendedTrustManager trustManager, Optional<X509KeyManager> keyManager) {
        final TlsContext tlsContext = new TlsContext() {
            @Override
            public String getProtocol() {
                return sslContext.getProtocol();
            }

            @Override
            public X509TrustManager getTrustManager() {
                return trustManager;
            }

            @Override
            public Optional<X509KeyManager> getKeyManager() {
                return keyManager;
            }
        };

        final StandardWebClientService client = new StandardWebClientService();
        client.setHttpVersion(HttpClient.Version.HTTP_1_1);
        client.setTlsContext(tlsContext);
        return client;
    }
}
