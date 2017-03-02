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
package org.apache.nifi.diagnostics;

/**
 * Disk usage for a file system directory.
 *
 */
public class StorageUsage implements Cloneable {

    private String identifier;

    private long freeSpace;
    private long totalSpace;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(final String identifier) {
        this.identifier = identifier;
    }

    public void setFreeSpace(final long freeSpace) {
        this.freeSpace = freeSpace;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public long getTotalSpace() {
        return totalSpace;
    }

    public void setTotalSpace(final long totalSpace) {
        this.totalSpace = totalSpace;
    }

    public long getUsedSpace() {
        return totalSpace - freeSpace;
    }

    public int getDiskUtilization() {
        return DiagnosticUtils.getUtilization(getUsedSpace(), totalSpace);
    }

    @Override
    public StorageUsage clone() {
        final StorageUsage clonedObj = new StorageUsage();
        clonedObj.identifier = identifier;
        clonedObj.freeSpace = freeSpace;
        clonedObj.totalSpace = totalSpace;
        return clonedObj;
    }

}
