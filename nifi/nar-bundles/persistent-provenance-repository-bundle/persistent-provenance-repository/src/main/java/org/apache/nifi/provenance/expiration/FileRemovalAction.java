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

import java.io.File;
import java.io.IOException;

import org.apache.nifi.provenance.lucene.DeleteIndexAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileRemovalAction implements ExpirationAction {

    private static final Logger logger = LoggerFactory.getLogger(DeleteIndexAction.class);

    @Override
    public File execute(final File expiredFile) throws IOException {
        boolean removed = false;
        for (int i = 0; i < 10 && !removed; i++) {
            if ((removed = expiredFile.delete())) {
                logger.info("Removed expired Provenance Event file {}", expiredFile);
                return null;
            }
        }

        logger.warn("Failed to remove old Provenance Event file {}", expiredFile);
        return expiredFile;
    }

    @Override
    public boolean hasBeenPerformed(final File expiredFile) throws IOException {
        return !expiredFile.exists();
    }
}
