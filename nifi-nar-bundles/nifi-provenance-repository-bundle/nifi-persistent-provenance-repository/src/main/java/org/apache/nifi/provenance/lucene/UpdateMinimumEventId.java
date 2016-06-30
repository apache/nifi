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

package org.apache.nifi.provenance.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.nifi.provenance.IndexConfiguration;
import org.apache.nifi.provenance.expiration.ExpirationAction;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateMinimumEventId implements ExpirationAction {
    private static final Logger logger = LoggerFactory.getLogger(UpdateMinimumEventId.class);

    private final IndexConfiguration indexConfig;

    public UpdateMinimumEventId(final IndexConfiguration indexConfig) {
        this.indexConfig = indexConfig;
    }

    @Override
    public File execute(final File expiredFile) throws IOException {
        try (final RecordReader reader = RecordReaders.newRecordReader(expiredFile, null, Integer.MAX_VALUE)) {
            final long maxEventId = reader.getMaxEventId();
            indexConfig.setMinIdIndexed(maxEventId);

            logger.info("Updated Minimum Event ID for Provenance Event Repository - Minimum Event ID now {}", maxEventId);
        } catch (final IOException ioe) {
            logger.warn("Failed to obtain max ID present in journal file {}", expiredFile.getAbsolutePath());
        }

        return expiredFile;
    }

    @Override
    public boolean hasBeenPerformed(final File expiredFile) throws IOException {
        return !expiredFile.exists();
    }
}
