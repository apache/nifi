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
package org.apache.nifi.processors.hadoop.util.writer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;

import java.util.List;

/**
 * Interface for common management of writing to records and to flowfiles.
 */
public interface HdfsObjectWriter {

    void write();

    long getListedFileCount();

    default boolean determineListable(final FileStatus status, final long minimumAge, final long maximumAge, final PathFilter filter,
                                      final long latestModificationTime, final List<String> latestModifiedStatuses) {
        final boolean isCopyInProgress = status.getPath().getName().endsWith("_COPYING_");
        final boolean isFilterAccepted = filter.accept(status.getPath());
        if (isCopyInProgress || !isFilterAccepted) {
            return false;
        }
        // If the file was created during the processor's last iteration we have to check if it was already listed
        if (status.getModificationTime() == latestModificationTime) {
            return !latestModifiedStatuses.contains(status.getPath().toString());
        }

        final long fileAge = System.currentTimeMillis() - status.getModificationTime();
        if (minimumAge > fileAge || fileAge > maximumAge) {
            return false;
        }

        return status.getModificationTime() > latestModificationTime;
    }
}
