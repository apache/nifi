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
package org.apache.nifi.kafka.service.security;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.nifi.ssl.SSLContextProvider;

import java.security.KeyStore;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * Standard implementation of Apache Kafka SslEngineFactory using Apache NiFi SSLContextProvider Controller Services
 */
public class StandardSslEngineFactory implements SslEngineFactory {

    public static final String SSL_CONTEXT_PROVIDER_PROPERTY = SSLContextProvider.class.getName();

    private volatile SSLContext sslContext;

    /**
     * Configure the Factory using provided properties
     *
     * @param configuration Map of properties with SSLContextProvider property required
     */
    @Override
    public void configure(final Map<String, ?> configuration) {
        final Object provider = configuration.get(SSL_CONTEXT_PROVIDER_PROPERTY);
        if (provider == null) {
            throw new KafkaException("Required property [%s] not configured".formatted(SSL_CONTEXT_PROVIDER_PROPERTY));
        }

        if (provider instanceof final SSLContextProvider sslContextProvider) {
            sslContext = sslContextProvider.createContext();
        } else {
            throw new KafkaException("Required property [%s] not valid [%s]".formatted(SSL_CONTEXT_PROVIDER_PROPERTY, provider.getClass().getName()));
        }
    }

    /**
     * Create Client SSLEngine for Peer Address using provided Endpoint Identification Algorithm
     *
     * @param peerHost Peer host address
     * @param peerPort Peer port number
     * @param endpointIdentificationAlgorithm Endpoint identification algorithm for client mode verification
     * @return SSLEngine created according to provided properties
     */
    @Override
    public SSLEngine createClientSslEngine(final String peerHost, final int peerPort, final String endpointIdentificationAlgorithm) {
        final SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
        sslEngine.setUseClientMode(true);

        final SSLParameters sslParameters = sslEngine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm(endpointIdentificationAlgorithm);
        sslEngine.setSSLParameters(sslParameters);

        return sslEngine;
    }

    /**
     * Create Server SSLEngine is not supported
     *
     * @param peerHost Peer host address
     * @param peerPort Peer port number
     * @throws UnsupportedOperationException indicating Server SSLEngine creation not supported
     */
    @Override
    public SSLEngine createServerSslEngine(final String peerHost, final int peerPort) {
        throw new UnsupportedOperationException("Server SSLEngine creation not supported");
    }

    /**
     * Should be rebuilt based on proposed configuration
     *
     * @param configuration Proposed configuration properties
     * @return Rebuild is never required or supported
     */
    @Override
    public boolean shouldBeRebuilt(final Map<String, Object> configuration) {
        return false;
    }

    /**
     * Get reconfiguration properties returns an empty set indicating nothing can be reconfigured
     *
     * @return Empty set of property names
     */
    @Override
    public Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    /**
     * Get Keystore is not supported with the SSLContextProvider
     *
     * @return Null value indicating unsupported status
     */
    @Override
    public KeyStore keystore() {
        return null;
    }

    /**
     * Get Truststore is not supported with the SSLContextProvider
     *
     * @return Null value indicating unsupported status
     */
    @Override
    public KeyStore truststore() {
        return null;
    }

    @Override
    public void close() {

    }
}
