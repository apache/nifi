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

package org.apache.nifi.minifi.nar;

import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

public class NarAutoUnloadService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoUnloadService.class);
    private static final String UNPACKED_POSTFIX = "-unpacked";

    private final ExtensionManager extensionManager;
    private final File extensionWorkDirectory;
    private final NarLoader narLoader;

    public NarAutoUnloadService(ExtensionManager extensionManager, File extensionWorkDirectory, NarLoader narLoader) {
        this.extensionManager = extensionManager;
        this.extensionWorkDirectory = extensionWorkDirectory;
        this.narLoader = narLoader;
    }

    public void unloadNarFile(String fileName) {
        if (isSupported(fileName)) {
            File narWorkingDirectory = new File(extensionWorkDirectory, fileName + UNPACKED_POSTFIX);
            extensionManager.getAllBundles().stream()
                    .filter(bundle -> bundle.getBundleDetails().getWorkingDirectory().getPath().equals(narWorkingDirectory.getPath()))
                    .findFirst()
                    .ifPresentOrElse(narLoader::unload, () -> LOGGER.warn("NAR bundle not found for {}", fileName));
        }
    }

    private boolean isSupported(String fileName) {
        if (!fileName.endsWith(".nar")) {
            LOGGER.info("Skipping non-nar file {}", fileName);
            return false;
        } else if (fileName.startsWith(".")) {
            LOGGER.debug("Skipping partially written file {}", fileName);
            return false;
        }
        return true;
    }



}
