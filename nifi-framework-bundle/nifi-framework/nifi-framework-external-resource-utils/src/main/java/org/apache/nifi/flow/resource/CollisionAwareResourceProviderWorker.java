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
package org.apache.nifi.flow.resource;

import org.apache.nifi.nar.NarCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;

/**
 * It is important to note that this implementation prevents NiFi from catching up a file under writing. For example trying to load a NAR
 * file which is not fully acquired for example could lead to issues. In order to avoid this, the worker first creates a temporary
 * file and it will rename it to the expected name only after it has been successfully written to the disk.
 */
final class CollisionAwareResourceProviderWorker extends ConflictResolvingExternalResourceProviderWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(CollisionAwareResourceProviderWorker.class);

    CollisionAwareResourceProviderWorker(
        final String namePrefix,
        final ClassLoader providerClassLoader,
        final ExternalResourceProvider provider,
        final ExternalResourceConflictResolutionStrategy resolutionStrategy,
        final File targetDirectory,
        final long pollTimeInMs,
        final CountDownLatch restrainStartupLatch
    ) {
        super(namePrefix, providerClassLoader, provider, resolutionStrategy, targetDirectory, pollTimeInMs, restrainStartupLatch);
    }

    @Override
    protected void acquireResource(final ExternalResourceDescriptor availableResource) throws IOException {
        final long startedAt = System.currentTimeMillis();

        final File targetFile = new File(getTargetDirectory(), availableResource.getLocation());
        final File tempFile = new File(getTargetDirectory().getPath() + "/.provider_" + getId() + "_buffer.tmp");
        final File backupFile = new File(getTargetDirectory().getPath() + "/.provider_" + getId() + "_aside.tmp");

        try (
                final NarCloseable ignored = NarCloseable.withComponentNarLoader(getProviderClassLoader());
                final InputStream inputStream = getProvider().fetchExternalResource(availableResource);
        ) {
            if (tempFile.exists() && !tempFile.delete()) {
                throw new ExternalResourceProviderException("Buffer file '" + tempFile.getName() + "' already exists and cannot be deleted");
            }

            Files.copy(inputStream, tempFile.toPath());
        }

        if (targetFile.exists() && !targetFile.renameTo(backupFile)) {
            throw new ExternalResourceProviderException("Target file '" + targetFile.getName() + "' already exists and cannot be moved aside");
        }

        if (tempFile.renameTo(targetFile)) {
            LOGGER.info("Downloaded external resource {} in {} ms", availableResource.getLocation(), (System.currentTimeMillis() - startedAt));

            if (backupFile.exists() && !backupFile.delete()) {
                LOGGER.error("Could not remove set aside file for {}", targetFile.getName());
            }
        } else {
            LOGGER.error("Could not put downloaded resource {} in place", availableResource.getLocation());

            if (!tempFile.delete()) {
                LOGGER.error("Could not delete buffer file for {}", targetFile.getName());
            }

            if (backupFile.exists() && !backupFile.renameTo(targetFile)) {
                LOGGER.error("After failing to put new file in place, could not revert to the previous file");
            }
        }
    }
}
