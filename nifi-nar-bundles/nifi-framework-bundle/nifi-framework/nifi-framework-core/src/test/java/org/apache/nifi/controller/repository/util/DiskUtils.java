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
package org.apache.nifi.controller.repository.util;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;

import java.io.File;

public class DiskUtils {

    public static void deleteRecursively(final File toDelete, final int numTries) {
        File unableToDelete = null;
        for (int i = 0; i < numTries; i++) {
            unableToDelete = attemptRecursiveDelete(toDelete);
            if (unableToDelete == null) {
                return;
            }

            try {
                Thread.sleep(50L);
            } catch (final Exception e) {
            }
        }

        assertNull("Unable to delete " + unableToDelete, unableToDelete);
        assertFalse("Thought that I deleted " + toDelete + " but it still exists", toDelete.exists());
    }

    public static void deleteRecursively(final File toDelete) {
        final File unableToDelete = attemptRecursiveDelete(toDelete);
        assertNull("Unable to delete " + unableToDelete, unableToDelete);
        assertFalse("Thought that I deleted " + toDelete + " but it still exists", toDelete.exists());
    }

    private static File attemptRecursiveDelete(final File toDelete) {
        if (toDelete == null || !toDelete.exists()) {
            return null;
        }

        if (toDelete.isDirectory()) {
            for (final File file : toDelete.listFiles()) {
                final File unableToDelete = attemptRecursiveDelete(file);
                if (unableToDelete != null) {
                    return unableToDelete;
                }
            }
        }

        // try to delete up to 5 times.
        for (int i = 0; i < 5; i++) {
            if (!toDelete.exists() || toDelete.delete()) {
                return null;
            }

            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
            }
        }
        return toDelete;
    }
}
