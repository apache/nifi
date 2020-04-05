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
package org.apache.nifi.hdfs.repository;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.nifi.controller.repository.claim.ResourceClaim;

public class Container {

    private final String name;
    private final Path path;
    private final Configuration config;
    private final BlockingQueue<ResourceClaim> reclaimable = new ArrayBlockingQueue<>(10000);
    private final long minUsableSpaceForArchive;
    private final long fullThreshold;
    private final boolean failureDisabled;
    private final boolean disableChecksums;
    private volatile long lastFailure = 0;
    private volatile boolean failedRecently = false;
    private volatile boolean full = false;
    private volatile boolean active = true;

    public Container(String name, Path path, Configuration config, long minUsableSpaceForArchive, long fullThreshold, boolean pauseOnFailure)
            throws IOException {
        this.name = name;
        this.path = path;
        this.config = config;
        this.minUsableSpaceForArchive = minUsableSpaceForArchive;
        this.fullThreshold = fullThreshold;
        this.failureDisabled = !pauseOnFailure;
        this.disableChecksums = path.toString().startsWith("file:") || getFileSystem() instanceof RawLocalFileSystem;
    }

    public synchronized void setFull(boolean full) {
        this.full = full;
        if (full) {
            active = false;
        } else if (!failedRecently) {
            active = true;
        }
    }
    public void failureOcurred() {
        if (failureDisabled) {
            return;
        }
        lastFailure = System.currentTimeMillis();
        // avoid synchronizing in a critical path
        if (failedRecently) {
            return;
        }
        synchronized (this) {
            failedRecently = true;
            active = false;
        }
    }
    public synchronized boolean clearFailure(long expectedLastFailure) {
        if (lastFailure != expectedLastFailure) {
            return false;
        }
        failedRecently = false;
        if (!full) {
            active = true;
        }
        return true;
    }
    public long getLastFailure() {
        return lastFailure;
    }
    public boolean isActive() {
        return active;
    }
    public boolean isFailedRecently() {
        return failedRecently;
    }
    public boolean isFull() {
        return full;
    }
    public long getFullThreshold() {
        return fullThreshold;
    }
    public Configuration getConfig() {
        return config;
    }
    public Path getPath() {
        return path;
    }
    public String getName() {
        return name;
    }
    public long getMinUsableSpaceForArchive() {
        return minUsableSpaceForArchive;
    }
    public FileSystem getFileSystem() throws IOException {
        FileSystem fs = FileSystem.get(config);
        // I'm not sure if this needs to happen here, but it shouldn't be very expensive
        // we need these so LocalFileSystem doesn't put unexpected '.crc' files on the file system
        if (disableChecksums) {
            fs.setVerifyChecksum(false);
            fs.setWriteChecksum(false);
        }
        return fs;
    }
    public Path createPath(ResourceClaim claim) {
        return new Path(new Path(path, claim.getSection()), claim.getId());
    }
    public boolean addReclaimableFile(ResourceClaim claim) throws InterruptedException {
        return reclaimable.offer(claim, 10, TimeUnit.MINUTES);
    }
    public void drainReclaimable(List<ResourceClaim> drainTo) {
        reclaimable.drainTo(drainTo);
    }
    @Override
    public String toString() {
        return "[" + name + " -- " + path + "]";
    }
}
