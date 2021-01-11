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

package org.apache.nifi.stateless.config;

import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;

import javax.net.ssl.SSLContext;

public class SslConfigurationUtil {
    public static SSLContext createSslContext(final SslContextDefinition sslContextDefinition) throws StatelessConfigurationException {
        if (sslContextDefinition == null) {
            return null;
        }
        if (sslContextDefinition.getTruststoreFile() == null) {
            return null;
        }

        final TlsConfiguration tlsConfiguration = createTlsConfiguration(sslContextDefinition);

        try {
            return SslContextFactory.createSslContext(tlsConfiguration);
        } catch (final Exception e) {
            throw new StatelessConfigurationException("Failed to create SSL Context", e);
        }
    }

    public static TlsConfiguration createTlsConfiguration(final SslContextDefinition sslContextDefinition) {
        return new StandardTlsConfiguration(sslContextDefinition.getKeystoreFile(),
            sslContextDefinition.getKeystorePass(),
            sslContextDefinition.getKeyPass(),
            sslContextDefinition.getKeystoreType(),
            sslContextDefinition.getTruststoreFile(),
            sslContextDefinition.getTruststorePass(),
            sslContextDefinition.getTruststoreType(),
            TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
    }
}
