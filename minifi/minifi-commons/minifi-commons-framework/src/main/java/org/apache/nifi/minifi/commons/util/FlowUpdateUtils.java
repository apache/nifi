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

package org.apache.nifi.minifi.commons.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlowUpdateUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlowUpdateUtils.class);

    private FlowUpdateUtils() {
    }

    public static void backup(Path current, Path backup) throws IOException {
        if (current != null && exists(current)) {
            copy(current, backup, REPLACE_EXISTING);
        } else {
            LOGGER.warn("Flow configuration files does not exist. No backup copy will be created");
        }
    }

    public static void persist(byte[] flowConfig, Path flowPath, boolean compress) throws IOException {
        LOGGER.debug("Persisting flow to path {} with content\n{}\n and compress {}", flowPath, new String(flowConfig, UTF_8), compress);
        try (ByteArrayInputStream flowInputStream = new ByteArrayInputStream(flowConfig);
             OutputStream fileOut = compress
                 ? new GZIPOutputStream(newOutputStream(flowPath, WRITE, CREATE, TRUNCATE_EXISTING))
                 : newOutputStream(flowPath, WRITE, CREATE, TRUNCATE_EXISTING)) {
            flowInputStream.transferTo(fileOut);
        }
        LOGGER.info("Updated configuration was written to: {}", flowPath);
    }

    public static void revert(Path backupFlowConfigFile, Path currentFlowConfigFile) {
        if (backupFlowConfigFile != null && exists(backupFlowConfigFile)) {
            try {
                copy(backupFlowConfigFile, currentFlowConfigFile, REPLACE_EXISTING);
            } catch (IOException e) {
                LOGGER.error("Revert to previous flow failed. Please stop MiNiFi and revert the flow manually");
                throw new UncheckedIOException("Failed to revert flow", e);
            }
        } else {
            LOGGER.error("Backup flow configuration file does not exist");
            throw new RuntimeException("Backup flow configuration file does not exist");
        }
    }

    public static void removeIfExists(Path flowConfigFile) {
        if (flowConfigFile != null) {
            try {
                deleteIfExists(flowConfigFile);
            } catch (IOException e) {
                LOGGER.warn("Unable to remove flow configuration backup file", e);
            }
        }
    }
}
