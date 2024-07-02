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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Changed Path Listener loads new Key Manager and Trust Manager implementations from Path resources
 */
public class SecurityStoreChangedPathListener implements WatchServiceMonitorCommand.ChangedPathListener {
    private static final Logger logger = LoggerFactory.getLogger(SecurityStoreChangedPathListener.class);

    private final Set<Path> storeFileNames;

    private final KeyManagerListener keyManagerListener;

    private final KeyManagerBuilder keyManagerBuilder;

    private final TrustManagerListener trustManagerListener;

    private final TrustManagerBuilder trustManagerBuilder;

    /**
     * Security Store Changed Path Listener reloads Key Manager and Trust Manager when changed paths contain files match store file names
     *
     * @param storeFileNames Key Store and Trust Store File Names that must be matched
     * @param keyManagerListener Key Manager Listener for handling updated Key Manager
     * @param keyManagerBuilder Key Manager Builder for creating new Key Manager instances
     * @param trustManagerListener Trust Manager Listener for handling updated Trust Manager
     * @param trustManagerBuilder Trust Manager Builder for creating new Trust Manager instances
     */
    public SecurityStoreChangedPathListener(
            final Set<Path> storeFileNames,
            final KeyManagerListener keyManagerListener,
            final KeyManagerBuilder keyManagerBuilder,
            final TrustManagerListener trustManagerListener,
            final TrustManagerBuilder trustManagerBuilder
    ) {
        this.storeFileNames = Objects.requireNonNull(storeFileNames, "Store File Names required");
        this.keyManagerListener = Objects.requireNonNull(keyManagerListener, "Key Manager Listener required");
        this.keyManagerBuilder = Objects.requireNonNull(keyManagerBuilder, "Key Manager Builder required");
        this.trustManagerListener = Objects.requireNonNull(trustManagerListener, "Trust Manager Listener required");
        this.trustManagerBuilder = Objects.requireNonNull(trustManagerBuilder, "Trust Manager Builder required");
    }

    @Override
    public void onChanged(final List<Path> changedPaths) {
        final Optional<Path> storeFileNameFound = changedPaths.stream()
                .map(Path::getFileName)
                .filter(storeFileNames::contains)
                .findFirst();

        if (storeFileNameFound.isPresent()) {
            final X509ExtendedKeyManager keyManager = keyManagerBuilder.build();
            final X509ExtendedTrustManager trustManager = trustManagerBuilder.build();

            keyManagerListener.setKeyManager(keyManager);
            trustManagerListener.setTrustManager(trustManager);
            logger.info("Key Manager and Trust Manager Reloaded from Changed Path [{}]", storeFileNameFound.get().toAbsolutePath());
        }
    }
}
