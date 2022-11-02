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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateAssetCommandHelper {

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

    public boolean assetUpdatePrecondition(String assetFileName, Boolean forceDownload) {
        Path assetPath = Paths.get(assetDirectory, assetFileName);
        if (Files.exists(assetPath) && !forceDownload) {
            LOG.info("Asset file already exists on path {}. Asset won't be downloaded", assetPath);
            return false;
        }
        LOG.info("Asset file does not exist or force download is on. Asset will be downloaded to {}", assetPath);
        return true;
    }

    public boolean assetPersistFunction(String assetFileName, byte[] assetBinary) {
        Path assetPath = Paths.get(assetDirectory, assetFileName);
        try {
            Files.deleteIfExists(assetPath);
            Files.write(assetPath, assetBinary);
            LOG.info("Asset was persisted to {}, {} bytes were written", assetPath, assetBinary.length);
            return true;
        } catch (IOException e) {
            LOG.error("Persisting asset failed. File creation was not successful targeting {}", assetPath, e);
            return false;
        }
    }
}
