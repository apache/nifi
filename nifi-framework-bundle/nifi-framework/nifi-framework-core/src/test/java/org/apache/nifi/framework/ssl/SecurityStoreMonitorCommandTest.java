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
package org.apache.nifi.framework.ssl;

import org.apache.nifi.security.ssl.KeyManagerBuilder;
import org.apache.nifi.security.ssl.KeyManagerListener;
import org.apache.nifi.security.ssl.TrustManagerBuilder;
import org.apache.nifi.security.ssl.TrustManagerListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SecurityStoreMonitorCommandTest {
    private static final String KEY_STORE_FILE_NAME = "keystore.p12";

    private static final String INITIAL_CONTENT = "initial-store";

    private static final String UPDATED_CONTENT = "updated-store";

    private static final String RELOADED_CONTENT = "reloaded-store";

    @TempDir
    private Path tempDir;

    @Mock
    private KeyManagerListener keyManagerListener;

    @Mock
    private KeyManagerBuilder keyManagerBuilder;

    @Mock
    private TrustManagerListener trustManagerListener;

    @Mock
    private TrustManagerBuilder trustManagerBuilder;

    @Mock
    private X509ExtendedKeyManager keyManager;

    @Mock
    private X509ExtendedTrustManager trustManager;

    @Test
    void testRunStoreUnchanged() throws IOException {
        final Path keyStorePath = writeStore(INITIAL_CONTENT);
        final SecurityStoreMonitorCommand command = newCommand(keyStorePath);

        command.run();

        verifyNoInteractions(keyManagerBuilder, keyManagerListener, trustManagerBuilder, trustManagerListener);
    }

    @Test
    void testRunStoreRewrittenReloadsManagers() throws IOException {
        final Path keyStorePath = writeStore(INITIAL_CONTENT);
        final SecurityStoreMonitorCommand command = newCommand(keyStorePath);
        setManagers();

        writeStore(UPDATED_CONTENT);
        command.run();

        verify(keyManagerListener).setKeyManager(keyManager);
        verify(trustManagerListener).setTrustManager(trustManager);
    }

    @Test
    void testRunStoreRewrittenWithIdenticalContent() throws IOException {
        final Path keyStorePath = writeStore(INITIAL_CONTENT);
        final SecurityStoreMonitorCommand command = newCommand(keyStorePath);

        writeStore(INITIAL_CONTENT);
        command.run();

        verifyNoInteractions(keyManagerBuilder, keyManagerListener, trustManagerBuilder, trustManagerListener);
    }

    @Test
    void testRunReloadFailureDoesNotPropagate() throws IOException {
        final Path keyStorePath = writeStore(INITIAL_CONTENT);
        final SecurityStoreMonitorCommand command = newCommand(keyStorePath);

        doThrow(new IllegalStateException("Key Store loading failed")).when(keyManagerBuilder).build();
        writeStore(UPDATED_CONTENT);
        command.run();

        verifyNoInteractions(keyManagerListener, trustManagerListener);

        doReturn(keyManager).when(keyManagerBuilder).build();
        doReturn(trustManager).when(trustManagerBuilder).build();
        writeStore(RELOADED_CONTENT);
        command.run();

        verify(keyManagerListener).setKeyManager(keyManager);
        verify(trustManagerListener).setTrustManager(trustManager);
    }

    @Test
    void testRunDeletedStoreDoesNotPropagate() throws IOException {
        final Path keyStorePath = writeStore(INITIAL_CONTENT);
        final SecurityStoreMonitorCommand command = newCommand(keyStorePath);

        Files.delete(keyStorePath);
        command.run();

        verifyNoInteractions(keyManagerBuilder, keyManagerListener, trustManagerBuilder, trustManagerListener);
    }

    private SecurityStoreMonitorCommand newCommand(final Path storePath) {
        return new SecurityStoreMonitorCommand(
                Set.of(storePath),
                keyManagerListener,
                keyManagerBuilder,
                trustManagerListener,
                trustManagerBuilder
        );
    }

    private void setManagers() {
        when(keyManagerBuilder.build()).thenReturn(keyManager);
        when(trustManagerBuilder.build()).thenReturn(trustManager);
    }

    private Path writeStore(final String content) throws IOException {
        final Path storePath = tempDir.resolve(KEY_STORE_FILE_NAME);
        Files.writeString(storePath, content, StandardCharsets.UTF_8);
        return storePath;
    }
}
