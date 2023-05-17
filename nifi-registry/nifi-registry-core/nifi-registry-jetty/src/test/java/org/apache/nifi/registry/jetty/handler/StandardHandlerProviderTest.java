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
package org.apache.nifi.registry.jetty.handler;

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.crypto.CryptoKeyProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class StandardHandlerProviderTest {
    @Mock
    private CryptoKeyProvider cryptoKeyProvider;

    private StandardHandlerProvider provider;

    @BeforeEach
    void setProvider(@TempDir final File tempDir) {
        final String docsDirectory = tempDir.getAbsolutePath();
        provider = new StandardHandlerProvider(cryptoKeyProvider, docsDirectory);
    }

    @Test
    void testGetHandlerApplicationNotFound() {
        final NiFiRegistryProperties properties = new NiFiRegistryProperties();

        assertThrows(IllegalStateException.class, () -> provider.getHandler(properties));
    }
}
