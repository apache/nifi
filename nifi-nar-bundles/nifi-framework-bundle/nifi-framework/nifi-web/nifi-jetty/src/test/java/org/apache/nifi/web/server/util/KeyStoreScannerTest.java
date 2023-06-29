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
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.function.Consumer;

public class KeyStoreScannerTest {
    private StoreScanner scanner;
    private SslContextFactory sslContextFactory;
    private static TlsConfiguration tlsConfiguration;
    private static File keyStoreFile;

    @BeforeAll
    public static void initClass() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        keyStoreFile = Paths.get(tlsConfiguration.getKeystorePath()).toFile();
    }

    @BeforeEach
    public void init() throws IOException {
        sslContextFactory = Mockito.mock(SslContextFactory.class);
        Resource keyStoreResource = Mockito.mock(Resource.class);
        Mockito.when(keyStoreResource.getFile()).thenReturn(keyStoreFile);
        Mockito.when(sslContextFactory.getKeyStoreResource()).thenReturn(keyStoreResource);

        scanner = new StoreScanner(sslContextFactory, tlsConfiguration, sslContextFactory.getKeyStoreResource());
    }

    @Test
    public void testFileAdded() throws Exception {
        scanner.fileAdded(keyStoreFile.getAbsolutePath());

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        Mockito.verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

        final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
        consumer.accept(sslContextFactory);
        Mockito.verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
    }

    @Test
    public void testFileChanged() throws Exception {
        scanner.fileChanged(keyStoreFile.getAbsolutePath());

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        Mockito.verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

        final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
        consumer.accept(sslContextFactory);
        Mockito.verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
    }

    @Test
    public void testFileRemoved() throws Exception {
        scanner.fileRemoved(keyStoreFile.getAbsolutePath());

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        Mockito.verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

        final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
        consumer.accept(sslContextFactory);
        Mockito.verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
    }

    @Test
    public void testReload() throws Exception {
        scanner.reload();

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        Mockito.verify(sslContextFactory).reload(consumerArgumentCaptor.capture());

        final Consumer<SslContextFactory> consumer = consumerArgumentCaptor.getValue();
        consumer.accept(sslContextFactory);
        Mockito.verify(sslContextFactory).setSslContext(ArgumentMatchers.any(SSLContext.class));
    }
}
