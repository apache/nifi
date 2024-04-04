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
import java.util.Collections;
import java.util.List;

/**
 * Keeps a list of the latest modified file paths and the latest modified timestamp of the current run.
 */
public class FileStatusManager {

    private final List<String> currentLatestFiles;
    private long currentLatestTimestamp;

    public FileStatusManager(final long initialLatestTimestamp, final List<String> initialLatestFiles) {
        currentLatestTimestamp = initialLatestTimestamp;
        currentLatestFiles = new ArrayList<>(initialLatestFiles);
    }

    public void update(final FileStatus status) {
        if (status.getModificationTime() > currentLatestTimestamp) {
            currentLatestTimestamp = status.getModificationTime();
            currentLatestFiles.clear();
            currentLatestFiles.add(status.getPath().toString());
        } else if (status.getModificationTime() == currentLatestTimestamp) {
            currentLatestFiles.add(status.getPath().toString());
        }
    }

    public List<String> getCurrentLatestFiles() {
        return Collections.unmodifiableList(currentLatestFiles);
    }

    public long getCurrentLatestTimestamp() {
        return currentLatestTimestamp;
    }
}
