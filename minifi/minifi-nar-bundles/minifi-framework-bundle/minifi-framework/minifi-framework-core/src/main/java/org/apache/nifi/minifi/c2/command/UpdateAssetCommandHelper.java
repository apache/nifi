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

package org.apache.nifi.minifi.c2.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

public class UpdateAssetCommandHelper {

    public static final Pattern ALLOWED_RESOURCE_PATH_PATTERN = compile("^(?:[^~<>:|\"?*./\\\\]+(?:[/\\\\][^~<>:|\"?*./\\\\]+)*)?$");

    private static final Logger LOG = LoggerFactory.getLogger(UpdateAssetCommandHelper.class);

    private final String assetDirectory;

    public UpdateAssetCommandHelper(String assetDirectory) {
        this.assetDirectory = assetDirectory;
    }

    public void createAssetDirectory() {
        try {
            Files.createDirectories(Paths.get(assetDirectory));
        } catch (IOException ioe) {
            LOG.error("Unable to create asset directory {}", assetDirectory);
            throw new UncheckedIOException("Unable to create directory", ioe);
        }
    }

    public boolean assetUpdatePrecondition(final String assetFileName, final Boolean forceDownload) {
        final boolean downloadEnabled;

        final Matcher assetFileNameMatcher = ALLOWED_RESOURCE_PATH_PATTERN.matcher(assetFileName);
        if (assetFileNameMatcher.matches()) {
            final Path assetPath = Paths.get(assetDirectory, assetFileName);
            if (Files.exists(assetPath) && !forceDownload) {
                LOG.info("Asset File found at [{}] Download disabled", assetPath);
                downloadEnabled = false;
            } else {
                LOG.info("Asset File not found at [{}] or Force Download enabled", assetPath);
                downloadEnabled = true;
            }
        } else {
            LOG.warn("Asset File Name [{}] not allowed for downloading", assetFileName);
            downloadEnabled = false;
        }

        return downloadEnabled;
    }

    public boolean assetPersistFunction(final String assetFileName, final byte[] assetBinary) {
        boolean persisted;

        final Matcher assetFileNameMatcher = ALLOWED_RESOURCE_PATH_PATTERN.matcher(assetFileName);
        if (assetFileNameMatcher.matches()) {
            final Path assetPath = Paths.get(assetDirectory, assetFileName);
            try {
                Files.deleteIfExists(assetPath);
                Files.write(assetPath, assetBinary);
                LOG.info("Asset was persisted to {}, {} bytes were written", assetPath, assetBinary.length);
                persisted = true;
            } catch (final IOException e) {
                LOG.error("Persisting asset failed. File creation was not successful targeting {}", assetPath, e);
                persisted = false;
            }
        } else {
            LOG.warn("Asset File Name [{}] not allowed for writing", assetFileName);
            persisted = false;
        }

        return persisted;
    }
}
