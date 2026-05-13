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
package org.apache.nifi.kafka.service;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Kafka3ConnectionServiceSSLIT extends Kafka3ConnectionServiceBaseIT {

    private static final String TLS_PROTOCOL = "TLS";

    @Override
    protected Map<String, String> getKafkaContainerConfigProperties() {
        final Map<String, String> properties = new LinkedHashMap<>(super.getKafkaContainerConfigProperties());
        properties.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:SSL,PLAINTEXT:SSL,CONTROLLER:SSL");
        properties.put("KAFKA_SSL_KEYSTORE_LOCATION", keyStorePath.toString());
        properties.put("KAFKA_SSL_KEYSTORE_TYPE", keyStoreType);
        properties.put("KAFKA_SSL_KEYSTORE_PASSWORD", KEY_STORE_PASSWORD);
        properties.put("KAFKA_SSL_KEY_PASSWORD", KEY_PASSWORD);
        properties.put("KAFKA_SSL_TRUSTSTORE_LOCATION", trustStorePath.toString());
        properties.put("KAFKA_SSL_TRUSTSTORE_TYPE", keyStoreType);
        properties.put("KAFKA_SSL_TRUSTSTORE_PASSWORD", KEY_STORE_PASSWORD);
        properties.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false");
        properties.put("KAFKA_SSL_CLIENT_AUTH", "required");
        properties.put("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", " ");
        return properties;
    }

    @Override
    protected Map<String, String> getKafkaServiceConfigProperties() throws InitializationException {
        final Map<String, String> properties = new LinkedHashMap<>(super.getKafkaServiceConfigProperties());
        properties.put(Kafka3ConnectionService.SECURITY_PROTOCOL.getName(), SecurityProtocol.SSL.name());
        properties.put(Kafka3ConnectionService.SSL_CONTEXT_SERVICE.getName(), addSSLContextService(runner));
        return properties;
    }

    @Override
    protected Map<String, String> getAdminClientConfigProperties() {
        final Map<String, String> properties = new LinkedHashMap<>(super.getAdminClientConfigProperties());
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
        properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD);
        properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath.toString());
        properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keyStoreType);
        properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEY_STORE_PASSWORD);
        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath.toString());
        properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, keyStoreType);
        properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, KEY_STORE_PASSWORD);
        return properties;
    }

    private String addSSLContextService(final TestRunner runner) throws InitializationException {
        final String identifier = SSLContextService.class.getSimpleName();
        final SSLContextProvider provider = mock(SSLContextProvider.class);
        when(provider.getIdentifier()).thenReturn(identifier);

        runner.addControllerService(identifier, provider);
        runner.enableControllerService(provider);

        try {
            final SSLContext sslContext = buildSslContext();
            when(provider.createContext()).thenReturn(sslContext);
        } catch (final GeneralSecurityException | IOException e) {
            throw new InitializationException(e);
        }

        return identifier;
    }

    private SSLContext buildSslContext() throws GeneralSecurityException, IOException {
        final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        try (InputStream inputStream = Files.newInputStream(keyStorePath)) {
            keyStore.load(inputStream, KEY_STORE_PASSWORD.toCharArray());
        }
        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEY_PASSWORD.toCharArray());

        final KeyStore trustStore = KeyStore.getInstance(keyStoreType);
        try (InputStream inputStream = Files.newInputStream(trustStorePath)) {
            trustStore.load(inputStream, KEY_STORE_PASSWORD.toCharArray());
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        final SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
        final SecureRandom secureRandom = new SecureRandom();
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), secureRandom);
        return sslContext;
    }
}
