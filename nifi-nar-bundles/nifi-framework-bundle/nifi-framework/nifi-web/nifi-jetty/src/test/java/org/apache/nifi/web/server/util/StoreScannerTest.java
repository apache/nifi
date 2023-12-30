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
package org.apache.nifi.web.server.util;

import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StoreScannerTest {
    private SslContextFactory sslContextFactory;
    private static TlsConfiguration tlsConfiguration;
    private static Path keyStoreFile;
    private static Path trustStoreFile;
    private static Map<Path, StoreScanner> filesToScannerMap;

    @Captor
    private ArgumentCaptor<Consumer<SslContextFactory>> consumerArgumentCaptor;

    @BeforeAll
    public static void initClass() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        keyStoreFile = Paths.get(tlsConfiguration.getKeystorePath());
        trustStoreFile = Paths.get(tlsConfiguration.getTruststorePath());
    }

    @BeforeEach
    public void init() {
        sslContextFactory = mock(SslContextFactory.class);
        Resource keyStoreResource = mock(Resource.class);
        when(keyStoreResource.getPath()).thenReturn(keyStoreFile);
        when(sslContextFactory.getKeyStoreResource()).thenReturn(keyStoreResource);

        Resource trustStoreResource = mock(Resource.class);
        when(trustStoreResource.getPath()).thenReturn(trustStoreFile);
        when(sslContextFactory.getTrustStoreResource()).thenReturn(trustStoreResource);

        final StoreScanner keyStoreScanner = new StoreScanner(sslContextFactory, tlsConfiguration, sslContextFactory.getKeyStoreResource());
        final StoreScanner trustStoreScanner = new StoreScanner(sslContextFactory, tlsConfiguration, sslContextFactory.getTrustStoreResource());
        filesToScannerMap = new HashMap<>();
        filesToScannerMap.put(keyStoreFile, keyStoreScanner);
        filesToScannerMap.put(trustStoreFile, trustStoreScanner);
    }

    @Test
    public void testFileAdded() throws Exception {
        for (final Map.Entry<Path, StoreScanner> entry : filesToScannerMap.entrySet()) {
            final Path file = entry.getKey();
            final StoreScanner scanner = entry.getValue();
            scanner.fileAdded(file.toAbsolutePath().toString());

            verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

            final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
            consumer.accept(sslContextFactory);
            verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
            clearInvocations(sslContextFactory);
        }
    }

    @Test
    public void testFileChanged() throws Exception {
        for (final Map.Entry<Path, StoreScanner> entry : filesToScannerMap.entrySet()) {
            final Path file = entry.getKey();
            final StoreScanner scanner = entry.getValue();
            scanner.fileChanged(file.toAbsolutePath().toString());

            verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

            final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
            consumer.accept(sslContextFactory);
            verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
            clearInvocations(sslContextFactory);
        }
    }

    @Test
    public void testFileRemoved() throws Exception {
        for (final Map.Entry<Path, StoreScanner> entry : filesToScannerMap.entrySet()) {
            final Path file = entry.getKey();
            final StoreScanner scanner = entry.getValue();
            scanner.fileRemoved(file.toAbsolutePath().toString());

            verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

            final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
            consumer.accept(sslContextFactory);
            verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
            clearInvocations(sslContextFactory);
        }
    }

    @Test
    public void testReload() throws Exception {
        for (final Map.Entry<Path, StoreScanner> entry : filesToScannerMap.entrySet()) {
            final StoreScanner scanner = entry.getValue();
            scanner.reload();

            verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

            final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
            consumer.accept(sslContextFactory);
            verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
            clearInvocations(sslContextFactory);
        }
    }
}
