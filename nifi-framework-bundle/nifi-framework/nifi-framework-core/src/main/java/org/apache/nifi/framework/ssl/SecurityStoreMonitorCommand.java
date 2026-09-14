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
import org.apache.nifi.util.security.MessageDigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Runnable command that digests configured Key Store and Trust Store paths and reloads managers when content changes
 */
public class SecurityStoreMonitorCommand implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SecurityStoreMonitorCommand.class);

    private final Set<Path> storePaths;

    private final Map<Path, byte[]> storeDigests;

    private final KeyManagerListener keyManagerListener;

    private final KeyManagerBuilder keyManagerBuilder;

    private final TrustManagerListener trustManagerListener;

    private final TrustManagerBuilder trustManagerBuilder;

    /**
     * Security Store Monitor Command reloads Key Manager and Trust Manager when a configured store path digest changes
     *
     * @param storePaths Key Store and Trust Store Paths to digest
     * @param keyManagerListener Key Manager Listener for handling updated Key Manager
     * @param keyManagerBuilder Key Manager Builder for creating new Key Manager instances
     * @param trustManagerListener Trust Manager Listener for handling updated Trust Manager
     * @param trustManagerBuilder Trust Manager Builder for creating new Trust Manager instances
     */
    public SecurityStoreMonitorCommand(
            final Set<Path> storePaths,
            final KeyManagerListener keyManagerListener,
            final KeyManagerBuilder keyManagerBuilder,
            final TrustManagerListener trustManagerListener,
            final TrustManagerBuilder trustManagerBuilder
    ) {
        this.storePaths = Set.copyOf(Objects.requireNonNull(storePaths, "Store Paths required"));
        this.keyManagerListener = Objects.requireNonNull(keyManagerListener, "Key Manager Listener required");
        this.keyManagerBuilder = Objects.requireNonNull(keyManagerBuilder, "Key Manager Builder required");
        this.trustManagerListener = Objects.requireNonNull(trustManagerListener, "Trust Manager Listener required");
        this.trustManagerBuilder = Objects.requireNonNull(trustManagerBuilder, "Trust Manager Builder required");
        this.storeDigests = new HashMap<>();

        for (final Path storePath : this.storePaths) {
            try {
                storeDigests.put(storePath, getDigest(storePath));
            } catch (final IOException e) {
                logger.warn("Digest calculation failed for Security Store [{}]", storePath, e);
            }
        }
    }

    /**
     * Digest configured store paths and reload Key Manager and Trust Manager when content changes
     */
    @Override
    public void run() {
        final List<Path> changedPaths = new ArrayList<>();

        for (final Path storePath : storePaths) {
            try {
                final byte[] currentDigest = getDigest(storePath);
                // Set current digest regardless of potential issues with reading other paths
                final byte[] previousDigest = storeDigests.put(storePath, currentDigest);
                if (Arrays.equals(previousDigest, currentDigest)) {
                    logger.debug("Digest not changed for Store Path [{}]", storePath);
                } else {
                    changedPaths.add(storePath);
                }
            } catch (final IOException e) {
                logger.warn("Digest calculation failed for Security Store [{}]", storePath, e);
            }
        }

        if (changedPaths.isEmpty()) {
            logger.debug("Changed Security Store Paths not found");
        } else {
            try {
                final X509ExtendedKeyManager keyManager = keyManagerBuilder.build();
                final X509ExtendedTrustManager trustManager = trustManagerBuilder.build();
                keyManagerListener.setKeyManager(keyManager);
                trustManagerListener.setTrustManager(trustManager);
                logger.info("Key Manager and Trust Manager reloaded for changed Security Store Paths {}", changedPaths);
            } catch (final RuntimeException e) {
                logger.warn("Key Manager and Trust Manager reload failed for changed Security Store Paths {}", changedPaths, e);
            }
        }
    }

    private byte[] getDigest(final Path storePath) throws IOException {
        try (InputStream inputStream = Files.newInputStream(storePath)) {
            return MessageDigestUtils.getDigest(inputStream);
        }
    }
}
