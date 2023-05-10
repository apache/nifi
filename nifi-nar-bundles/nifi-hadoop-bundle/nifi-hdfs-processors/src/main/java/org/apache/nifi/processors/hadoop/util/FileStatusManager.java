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
package org.apache.nifi.processors.hadoop.util;

import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.List;

public class FileStatusManager {

    private List<FileStatus> lastModifiedStatusesFromLastIteration;
    private long lastModificationTimeFromLastIteration;
    private List<FileStatus> lastModifiedStatusesFromCurrentIteration;
    private long lastModificationTimeFromCurrentIteration;

    private FileStatusManager() {
        this.lastModificationTimeFromCurrentIteration = -1L;
        this.lastModificationTimeFromLastIteration = -1L;
        this.lastModifiedStatusesFromCurrentIteration = new ArrayList<>();
        this.lastModifiedStatusesFromLastIteration = new ArrayList<>();
    }

    public static FileStatusManager createFileStatusManager() {
        return new FileStatusManager();
    }

    public void update(final FileStatus status) {
        if (status != null && status.getModificationTime() > lastModificationTimeFromCurrentIteration) {
            lastModificationTimeFromCurrentIteration = status.getModificationTime();
            lastModifiedStatusesFromCurrentIteration.clear();
            lastModifiedStatusesFromCurrentIteration.add(status);
        } else if (status != null && status.getModificationTime() == lastModificationTimeFromCurrentIteration) {
            lastModifiedStatusesFromCurrentIteration.add(status);
        }
    }

    public void finishIteration() {
        lastModificationTimeFromLastIteration = lastModificationTimeFromCurrentIteration;
        lastModifiedStatusesFromLastIteration.clear();
        lastModifiedStatusesFromLastIteration.addAll(lastModifiedStatusesFromCurrentIteration);
        lastModifiedStatusesFromCurrentIteration.clear();
    }

    public List<FileStatus> getLastModifiedStatuses() {
        return lastModifiedStatusesFromLastIteration;
    }

    public long getLastModificationTime() {
        return lastModificationTimeFromLastIteration;
    }

    public void setLastModificationTime(long lastModificationTime) {
        this.lastModificationTimeFromLastIteration = lastModificationTime;
    }
}
