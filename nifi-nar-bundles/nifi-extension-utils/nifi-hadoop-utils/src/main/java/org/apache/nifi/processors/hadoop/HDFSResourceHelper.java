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
package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

public final class HDFSResourceHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSResourceHelper.class);

    private HDFSResourceHelper() {
        // Not to be instantiated
    }

    public static void closeFileSystem(final FileSystem fileSystem) {
        try {
            interruptStatisticsThread(fileSystem);
        } catch (Exception e) {
            LOGGER.warn("Error stopping FileSystem statistics thread: " + e.getMessage());
            LOGGER.debug("", e);
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    LOGGER.warn("Error close FileSystem: " + e.getMessage(), e);
                }
            }
        }
    }

    private static void interruptStatisticsThread(final FileSystem fileSystem) throws NoSuchFieldException, IllegalAccessException {
        final Field statsField = FileSystem.class.getDeclaredField("statistics");
        statsField.setAccessible(true);

        final Object statsObj = statsField.get(fileSystem);
        if (statsObj instanceof FileSystem.Statistics) {
            final FileSystem.Statistics statistics = (FileSystem.Statistics) statsObj;

            final Field statsThreadField = statistics.getClass().getDeclaredField("STATS_DATA_CLEANER");
            statsThreadField.setAccessible(true);

            final Object statsThreadObj = statsThreadField.get(statistics);
            if (statsThreadObj instanceof Thread) {
                final Thread statsThread = (Thread) statsThreadObj;
                try {
                    statsThread.interrupt();
                } catch (Exception e) {
                    LOGGER.warn("Error interrupting thread: " + e.getMessage(), e);
                }
            }
        }
    }
}
