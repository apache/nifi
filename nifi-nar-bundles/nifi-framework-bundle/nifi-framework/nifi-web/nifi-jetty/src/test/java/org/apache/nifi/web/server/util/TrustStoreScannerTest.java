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

import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsConfiguration;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.function.Consumer;

public class TrustStoreScannerTest {

    private TrustStoreScanner scanner;
    private SslContextFactory sslContextFactory;
    private static File keyStoreFile;
    private static File trustStoreFile;

    @BeforeClass
    public static void initClass() throws GeneralSecurityException, IOException {
        TlsConfiguration tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();
        keyStoreFile = Paths.get(tlsConfiguration.getKeystorePath()).toFile();
        trustStoreFile = Paths.get(tlsConfiguration.getTruststorePath()).toFile();
    }

    @Before
    public void init() throws IOException {
        sslContextFactory = Mockito.mock(SslContextFactory.class);
        Resource trustStoreResource = Mockito.mock(Resource.class);
        Mockito.when(trustStoreResource.getFile()).thenReturn(trustStoreFile);
        Mockito.when(sslContextFactory.getTrustStoreResource()).thenReturn(trustStoreResource);

        scanner = new TrustStoreScanner(sslContextFactory);
    }

    @Test
    public void fileAdded() throws Exception {
        scanner.fileAdded(trustStoreFile.getAbsolutePath());

        Mockito.verify(sslContextFactory).reload(ArgumentMatchers.any(Consumer.class));
    }

    @Test
    public void fileChanged() throws Exception {
        scanner.fileChanged(trustStoreFile.getAbsolutePath());

        Mockito.verify(sslContextFactory).reload(ArgumentMatchers.any(Consumer.class));
    }

    @Test
    public void fileRemoved() throws Exception {
        scanner.fileRemoved(trustStoreFile.getAbsolutePath());

        Mockito.verify(sslContextFactory).reload(ArgumentMatchers.any(Consumer.class));
    }

    @Test
    public void reload() throws Exception {
        scanner.reload();

        Mockito.verify(sslContextFactory).reload(ArgumentMatchers.any(Consumer.class));
    }

    @AfterClass
    public static void tearDown() throws IOException {
        Files.deleteIfExists(keyStoreFile.toPath());
        Files.deleteIfExists(trustStoreFile.toPath());
    }
}
