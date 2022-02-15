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

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.standard.socket.ClientConfigurationException;
import org.apache.nifi.processors.standard.socket.ClientConnectException;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.apache.nifi.processors.standard.util.FTPTransfer.PORT;
import static org.apache.nifi.processors.standard.util.FTPTransfer.PROXY_TYPE;
import static org.apache.nifi.processors.standard.util.FTPTransfer.PROXY_HOST;
import static org.apache.nifi.processors.standard.util.FTPTransfer.PROXY_PORT;
import static org.apache.nifi.processors.standard.util.FTPTransfer.HTTP_PROXY_USERNAME;
import static org.apache.nifi.processors.standard.util.FTPTransfer.HTTP_PROXY_PASSWORD;
import static org.apache.nifi.processors.standard.util.FTPTransfer.PROXY_TYPE_DIRECT;
import static org.apache.nifi.processors.standard.util.FileTransfer.CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.standard.util.FileTransfer.DATA_TIMEOUT;
import static org.apache.nifi.processors.standard.util.FileTransfer.HOSTNAME;
import static org.apache.nifi.processors.standard.util.FileTransfer.USE_COMPRESSION;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.CIPHERS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.HOST_KEY_FILE;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.PROXY_CONFIGURATION_SERVICE;
import static org.apache.nifi.processors.standard.util.SFTPTransfer.STRICT_HOST_KEY_CHECKING;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardSSHClientProviderTest {
    private static final PropertyValue NULL_PROPERTY_VALUE = new MockPropertyValue(null);

    private static final PropertyValue BOOLEAN_TRUE_PROPERTY_VALUE = new MockPropertyValue(Boolean.TRUE.toString());

    private static final PropertyValue BOOLEAN_FALSE_PROPERTY_VALUE = new MockPropertyValue(Boolean.FALSE.toString());

    private static final PropertyValue TIMEOUT_PROPERTY_VALUE = new MockPropertyValue("2 s");

    private static final String LOCALHOST = "localhost";

    private static final PropertyValue HOSTNAME_PROPERTY = new MockPropertyValue(LOCALHOST);

    @Mock
    private PropertyContext context;

    private StandardSSHClientProvider provider;

    private int port;

    @BeforeEach
    public void setProvider() {
        when(context.getProperty(any())).thenReturn(BOOLEAN_TRUE_PROPERTY_VALUE);
        when(context.getProperty(CIPHERS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(KEY_ALGORITHMS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(KEY_EXCHANGE_ALGORITHMS_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(MESSAGE_AUTHENTICATION_CODES_ALLOWED)).thenReturn(NULL_PROPERTY_VALUE);

        when(context.getProperty(CONNECTION_TIMEOUT)).thenReturn(TIMEOUT_PROPERTY_VALUE);
        when(context.getProperty(DATA_TIMEOUT)).thenReturn(TIMEOUT_PROPERTY_VALUE);
        when(context.getProperty(STRICT_HOST_KEY_CHECKING)).thenReturn(BOOLEAN_FALSE_PROPERTY_VALUE);
        when(context.getProperty(HOST_KEY_FILE)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(USE_COMPRESSION)).thenReturn(BOOLEAN_FALSE_PROPERTY_VALUE);

        when(context.getProperty(PROXY_CONFIGURATION_SERVICE)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(PROXY_TYPE)).thenReturn(new MockPropertyValue(PROXY_TYPE_DIRECT));
        when(context.getProperty(PROXY_HOST)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(PROXY_PORT)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(HTTP_PROXY_USERNAME)).thenReturn(NULL_PROPERTY_VALUE);
        when(context.getProperty(HTTP_PROXY_PASSWORD)).thenReturn(NULL_PROPERTY_VALUE);

        when(context.getProperty(HOSTNAME)).thenReturn(HOSTNAME_PROPERTY);

        port = NetworkUtils.getAvailableTcpPort();
        when(context.getProperty(PORT)).thenReturn(new MockPropertyValue(Integer.toString(port)));

        provider = new StandardSSHClientProvider();
    }

    @Test
    public void testGetClientConfigurationException() {
        final ClientConfigurationException exception = assertThrows(ClientConfigurationException.class, () -> provider.getClient(context, Collections.emptyMap()));
        assertTrue(exception.getMessage().contains(LOCALHOST));
        assertTrue(exception.getMessage().contains(Integer.toString(port)));
    }

    @Test
    public void testGetClientConnectException() {
        final ClientConnectException exception = assertThrows(ClientConnectException.class, () -> provider.getClient(context, Collections.emptyMap()));
        assertTrue(exception.getMessage().contains(LOCALHOST));
        assertTrue(exception.getMessage().contains(Integer.toString(port)));
    }
}
