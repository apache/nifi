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
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;

import static java.util.Objects.requireNonNull;

public class NarAutoUnloaderTaskFactory {

    private final NiFiProperties properties;
    private final ExtensionManager extensionManager;
    private final NarLoader narLoader;

    public NarAutoUnloaderTaskFactory(NiFiProperties properties, ExtensionManager extensionManager, NarLoader narLoader) {
        this.properties = requireNonNull(properties);
        this.extensionManager = requireNonNull(extensionManager);
        this.narLoader = requireNonNull(narLoader);
    }

    public NarAutoUnloaderTask createNarAutoUnloaderTask() throws IOException {
        File autoLoadDir = properties.getNarAutoLoadDirectory();
        FileUtils.ensureDirectoryExistAndCanRead(autoLoadDir);
        WatchService watcher = FileSystems.getDefault().newWatchService();
        Path autoLoadPath = autoLoadDir.toPath();
        autoLoadPath.register(watcher, StandardWatchEventKinds.ENTRY_DELETE);
        NarAutoUnloadService narAutoUnloadService = new NarAutoUnloadService(extensionManager, properties.getExtensionsWorkingDirectory(), narLoader);

        return new NarAutoUnloaderTask(autoLoadPath, watcher, narAutoUnloadService);
    }
}
