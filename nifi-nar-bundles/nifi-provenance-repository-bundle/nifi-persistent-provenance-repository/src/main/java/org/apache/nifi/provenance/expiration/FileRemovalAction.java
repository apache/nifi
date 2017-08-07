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
package org.apache.nifi.provenance.expiration;

import org.apache.nifi.provenance.toc.TocUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileRemovalAction implements ExpirationAction {

    private static final Logger logger = LoggerFactory.getLogger(FileRemovalAction.class);

    @Override
    public File execute(final File expiredFile) throws IOException {
        final boolean removed = remove(expiredFile);
        if (removed) {
            logger.info("Removed expired Provenance Event file {}", expiredFile);
        } else {
            logger.warn("Failed to remove old Provenance Event file {}; this file should be cleaned up manually", expiredFile);
        }

        final File tocFile = TocUtil.getTocFile(expiredFile);
        if (remove(tocFile)) {
            logger.info("Removed expired Provenance Table-of-Contents file {}", tocFile);
        } else {
            logger.warn("Failed to remove old Provenance Table-of-Contents file {}; this file should be cleaned up manually", expiredFile);
        }

        return removed ? null : expiredFile;
    }

    private boolean remove(final File file) {
        boolean removed = false;
        for (int i = 0; i < 10 && !removed; i++) {
            if (removed = file.delete()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean hasBeenPerformed(final File expiredFile) throws IOException {
        return !expiredFile.exists();
    }
}
