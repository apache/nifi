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
package org.apache.nifi.processors.standard.ssh;

import net.schmizz.keepalive.KeepAlive;
import net.schmizz.keepalive.KeepAliveProvider;
import net.schmizz.sshj.Config;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.common.Factory;
import net.schmizz.sshj.connection.ConnectionImpl;
import net.schmizz.sshj.transport.Transport;
import net.schmizz.sshj.transport.cipher.Cipher;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.nifi.processors.standard.util.SFTPTransfer.CIPHERS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardSSHConfigProviderTest {
    private static final Config DEFAULT_CONFIG;

    private static final String FIRST_ALLOWED_CIPHER = "aes128-ctr";

    private static final String SECOND_ALLOWED_CIPHER = "aes256-cbc";

    private static final String ALLOWED_CIPHERS = String.format("%s,%s", FIRST_ALLOWED_CIPHER, SECOND_ALLOWED_CIPHER);

    private static final PropertyValue NULL_PROPERTY_VALUE = new MockPropertyValue(null);

    private static final int KEEP_ALIVE_ENABLED_INTERVAL = 5;

    private static final int KEEP_ALIVE_DISABLED_INTERVAL = 0;

    private static final String IDENTIFIER = UUID.randomUUID().toString();

    static {
        final DefaultConfig prioritizedConfig = new DefaultConfig();
        prioritizedConfig.prioritizeSshRsaKeyAlgorithm();
        DEFAULT_CONFIG = prioritizedConfig;
    }

    @Mock
    private PropertyContext context;

    @Mock
    private ConnectionImpl connection;

    @Mock
    private Transport transport;

    private StandardSSHConfigProvider provider;

    @BeforeEach
    public void setProvider() {
        when(transport.getConfig()).thenReturn(DEFAULT_CONFIG);
        when(connection.getTransport()).thenReturn(transport);

        provider = new StandardSSHConfigProvider();
    }

    @Test
    public void testGetConfigDefaultValues() {
        when(context.getProperty(USE_KEEPALIVE_ON_TIMEOUT)).thenReturn(new MockPropertyValue(Boolean.TRUE.toString()));
        when(context.getProperty(CIPHERS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(KEY_ALGORITHMS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(KEY_EXCHANGE_ALGORITHMS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(MESSAGE_AUTHENTICATION_CODES_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);

        final Config config = provider.getConfig(IDENTIFIER, context);

        assertNotNull(config);

        final KeepAliveProvider keepAliveProvider = config.getKeepAliveProvider();
        final KeepAlive keepAlive = keepAliveProvider.provide(connection);
        assertEquals(KEEP_ALIVE_ENABLED_INTERVAL, keepAlive.getKeepAliveInterval());

        assertNamedEquals(DEFAULT_CONFIG.getCipherFactories(), config.getCipherFactories());
        assertNamedEquals(DEFAULT_CONFIG.getKeyAlgorithms(), config.getKeyAlgorithms());
        assertNamedEquals(DEFAULT_CONFIG.getKeyExchangeFactories(), config.getKeyExchangeFactories());
        assertNamedEquals(DEFAULT_CONFIG.getMACFactories(), config.getMACFactories());
    }

    @Test
    public void testGetConfigCiphersAllowedKeepAliveDisabled() {
        when(context.getProperty(USE_KEEPALIVE_ON_TIMEOUT)).thenReturn(new MockPropertyValue(Boolean.FALSE.toString()));
        when(context.getProperty(CIPHERS_ALLOWED)).thenReturn(new MockPropertyValue(ALLOWED_CIPHERS));
        when(context.getProperty(KEY_ALGORITHMS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(KEY_EXCHANGE_ALGORITHMS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(MESSAGE_AUTHENTICATION_CODES_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);

        final Config config = provider.getConfig(IDENTIFIER, context);

        assertNotNull(config);

        final KeepAliveProvider keepAliveProvider = config.getKeepAliveProvider();
        final KeepAlive keepAlive = keepAliveProvider.provide(connection);
        assertEquals(KEEP_ALIVE_DISABLED_INTERVAL, keepAlive.getKeepAliveInterval());

        final Iterator<Factory.Named<Cipher>> cipherFactories = config.getCipherFactories().iterator();
        assertTrue(cipherFactories.hasNext());
        final Factory.Named<Cipher> firstCipherFactory = cipherFactories.next();
        assertEquals(FIRST_ALLOWED_CIPHER, firstCipherFactory.getName());
        final Factory.Named<Cipher> secondCipherFactory = cipherFactories.next();
        assertEquals(SECOND_ALLOWED_CIPHER, secondCipherFactory.getName());
        assertFalse(cipherFactories.hasNext());

        assertNamedEquals(DEFAULT_CONFIG.getKeyAlgorithms(), config.getKeyAlgorithms());
        assertNamedEquals(DEFAULT_CONFIG.getKeyExchangeFactories(), config.getKeyExchangeFactories());
        assertNamedEquals(DEFAULT_CONFIG.getMACFactories(), config.getMACFactories());
    }

    private <T> void assertNamedEquals(final List<Factory.Named<T>> expected, final List<Factory.Named<T>> actual) {
        assertEquals(expected.size(), actual.size());

        final Iterator<Factory.Named<T>> expectedValues = expected.iterator();
        final Iterator<Factory.Named<T>> actualValues = actual.iterator();
        while (expectedValues.hasNext()) {
            final Factory.Named<?> expectedValue = expectedValues.next();
            final Factory.Named<?> actualValue = actualValues.next();
            assertEquals(expectedValue.getName(), actualValue.getName());
        }
    }
}
