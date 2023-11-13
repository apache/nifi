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

package org.apache.nifi.kafka.connect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

public class WorkingDirectoryUtils {

    protected static final String NAR_UNPACKED_SUFFIX = "nar-unpacked";
    protected static final String HASH_FILENAME = "nar-digest";
    private static final Logger logger = LoggerFactory.getLogger(WorkingDirectoryUtils.class);

    /**
     * Goes through the nar/extensions and extensions directories within the working directory
     * and deletes every directory whose name ends in "nar-unpacked" and does not have a
     * "nar-digest" file in it.
     * @param workingDirectory File object pointing to the working directory.
     */
    public static void reconcileWorkingDirectory(final File workingDirectory) {
        purgeIncompleteUnpackedNars(new File(new File(workingDirectory, "nar"), "extensions"));
        purgeIncompleteUnpackedNars(new File(workingDirectory, "extensions"));
    }

    /**
     * Receives a directory as parameter and goes through every directory within it that ends in
     * "nar-unpacked". If a directory ending in "nar-unpacked" does not have a file named
     * "nar-digest" within it, it gets deleted with all of its contents.
     * @param directory A File object pointing to the directory that is supposed to contain
     *                  further directories whose name ends in "nar-unpacked".
     */
    public static void purgeIncompleteUnpackedNars(final File directory) {
        final File[] unpackedDirs = directory.listFiles(file -> file.isDirectory() && file.getName().endsWith(NAR_UNPACKED_SUFFIX));
        if (unpackedDirs == null || unpackedDirs.length == 0) {
            logger.debug("Found no unpacked NARs in {}", directory);
            if (logger.isDebugEnabled()) {
                logger.debug("Directory contains: {}", Arrays.deepToString(directory.listFiles()));
            }
            return;
        }

        for (final File unpackedDir : unpackedDirs) {
            final File narHashFile = new File(unpackedDir, HASH_FILENAME);
            if (narHashFile.exists()) {
                logger.debug("Already successfully unpacked {}", unpackedDir);
            } else {
                purgeDirectory(unpackedDir);
            }
        }
    }

    /**
     * Delete a directory with all of its contents.
     * @param directory The directory to be deleted.
     */
    public static void purgeDirectory(final File directory) {
        if (directory.exists()) {
            deleteRecursively(directory);
            logger.debug("Cleaned up {}", directory);
        }
    }

    private static void deleteRecursively(final File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            final File[] files = fileOrDirectory.listFiles();
            if (files != null) {
                for (final File file : files) {
                    deleteRecursively(file);
                }
            }
        }
        deleteQuietly(fileOrDirectory);
    }

    private static void deleteQuietly(final File file) {
        final boolean deleted = file.delete();
        if (!deleted) {
            logger.debug("Failed to cleanup temporary file {}", file);
        }
    }

}
