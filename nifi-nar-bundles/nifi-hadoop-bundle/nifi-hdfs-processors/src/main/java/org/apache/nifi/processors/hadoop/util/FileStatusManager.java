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

    private final List<String> lastModifiedStatuses;
    private long lastModificationTime;

    public FileStatusManager() {
        lastModificationTime = 0L;
        lastModifiedStatuses = new ArrayList<>();
    }

    public FileStatusManager(final long lastModificationTime, final List<String> lastModifiedStatuses) {
        this.lastModificationTime = lastModificationTime;
        this.lastModifiedStatuses = lastModifiedStatuses;
    }

    public void update(final FileStatus status) {
        if (status.getModificationTime() > lastModificationTime) {
            lastModificationTime = status.getModificationTime();
            lastModifiedStatuses.clear();
            lastModifiedStatuses.add(status.getPath().toString());
        } else if (status.getModificationTime() == lastModificationTime) {
            lastModifiedStatuses.add(status.getPath().toString());
        }
    }

    public String getLastModifiedStatusesAsString() {
        return String.join(" ", lastModifiedStatuses);
    }

    public long getLastModificationTime() {
        return lastModificationTime;
    }
}
