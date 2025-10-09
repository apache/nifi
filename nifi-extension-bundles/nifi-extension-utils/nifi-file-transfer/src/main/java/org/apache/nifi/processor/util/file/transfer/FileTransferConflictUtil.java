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
package org.apache.nifi.processor.util.file.transfer;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;

public final class FileTransferConflictUtil {
    private FileTransferConflictUtil() {
    }

    /**
     * Attempts to generate a unique filename by prefixing with an incrementing integer followed by a dot.
     * Returns null if a unique name could not be found within 99 attempts.
     */
    public static String generateUniqueFilename(final FileTransfer transfer,
                                                final String path,
                                                final String baseFileName,
                                                final FlowFile flowFile,
                                                final ComponentLog logger) throws IOException {
        boolean uniqueNameGenerated;
        String candidate = null;
        for (int i = 1; i < 100; i++) {
            final String possibleFileName = i + "." + baseFileName;
            final FileInfo renamedFileInfo = transfer.getRemoteFileInfo(flowFile, path, possibleFileName);
            uniqueNameGenerated = (renamedFileInfo == null);
            if (uniqueNameGenerated) {
                candidate = possibleFileName;
                logger.info("Attempting to resolve filename conflict for {} on the remote server by using a newly generated filename of: {}", flowFile, candidate);
                break;
            }
        }
        return candidate;
    }
}
