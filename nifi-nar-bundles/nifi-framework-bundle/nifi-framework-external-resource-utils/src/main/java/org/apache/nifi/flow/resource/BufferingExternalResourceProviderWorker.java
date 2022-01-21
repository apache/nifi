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
final class BufferingExternalResourceProviderWorker extends ConflictResolvingExternalResourceProviderWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferingExternalResourceProviderWorker.class);

    BufferingExternalResourceProviderWorker(
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

    protected void acquireResource(final ExternalResourceDescriptor availableResource) throws IOException {
        final long startedAt = System.currentTimeMillis();
        final InputStream inputStream;

        final File targetFile = new File(getTargetDirectory(), availableResource.getLocation());
        final File bufferFile = new File(getTargetDirectory().getPath() + "/.provider_" + getId() + "_buffer.tmp");
        final File setAsideFile = new File(getTargetDirectory().getPath() + "/.provider_" + getId() + "_aside.tmp");

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getProviderClassLoader())) {
            inputStream = getProvider().fetchExternalResource(availableResource);
        }

        if (bufferFile.exists() && !bufferFile.delete()) {
            throw new ExternalResourceProviderException("Buffer file '" + bufferFile.getName() +"' already exists and cannot be deleted");
        }

        Files.copy(inputStream, bufferFile.toPath());
        inputStream.close();

        if (targetFile.exists() && !targetFile.renameTo(setAsideFile)) {
            throw new ExternalResourceProviderException("Target file '" + targetFile.getName() +"' already exists and cannot be moved aside");
        }

        if (bufferFile.renameTo(targetFile)) {
            LOGGER.info("Downloaded external resource {} in {} ms", availableResource.getLocation(), (System.currentTimeMillis() - startedAt));

            if (setAsideFile.exists() && !setAsideFile.delete()) {
                LOGGER.error("Could not remove set aside file for {}", targetFile.getName());
            }
        } else {
            LOGGER.error("Could not put downloaded resource {} in place", availableResource.getLocation());

            if (!bufferFile.delete()) {
                LOGGER.error("Could not delete buffer file for {}", targetFile.getName());
            }

            if (setAsideFile.exists() && !setAsideFile.renameTo(targetFile)) {
                LOGGER.error("After failing to put new file in place, could not revert to the previous file");
            }
        }
    }
}
