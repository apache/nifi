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
package org.apache.nifi.processors.standard.ftp.filesystem;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DefaultVirtualFileSystem implements VirtualFileSystem {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<VirtualPath> existingPaths = new ArrayList<>();

    public DefaultVirtualFileSystem() {
        existingPaths.add(ROOT);
    }

    @Override
    public boolean mkdir(VirtualPath newFile) {
        lock.writeLock().lock();
        try {
            if (existingPaths.contains(newFile)) {
                return false;
            } else {
                if (existingPaths.contains(newFile.getParent())) {
                    existingPaths.add(newFile);
                    return true;
                } else {
                    return false;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean exists(VirtualPath virtualFile) {
        lock.readLock().lock();
        try {
            return existingPaths.contains(virtualFile);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean delete(VirtualPath virtualFile) {
        if (virtualFile.equals(ROOT)) { // Root cannot be deleted
            return false;
        }

        lock.writeLock().lock();
        try {
            if (existingPaths.contains(virtualFile)) {
                if (!hasSubDirectories(virtualFile)) {
                    return existingPaths.remove(virtualFile);
                }
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean hasSubDirectories(VirtualPath directory) {
        return existingPaths.stream().anyMatch(e -> isChildOf(directory, e));
    }

    private boolean isChildOf(VirtualPath parent, VirtualPath childCandidate) {
        if (childCandidate.equals(ROOT)) {
            return false;
        }
        return parent.equals(childCandidate.getParent());
    }

    @Override
    public List<VirtualPath> listChildren(VirtualPath parent) {
        List<VirtualPath> children;

        lock.readLock().lock();
        try {
            if (parent.equals(ROOT)) {
                children = existingPaths.stream()
                        .filter(existingPath -> (!existingPath.equals(ROOT) && (existingPath.getNameCount() == 1)))
                        .collect(Collectors.toList());
            } else {
                int parentNameCount = parent.getNameCount();
                children = existingPaths.stream()
                        .filter(existingPath -> (((existingPath.getParent() != null) && existingPath.getParent().equals(parent))
                                && (existingPath.getNameCount() == (parentNameCount + 1))))
                        .collect(Collectors.toList());
            }
        } finally {
            lock.readLock().unlock();
        }
        return children;
    }

    @Override
    public int getTotalNumberOfFiles() {
        lock.readLock().lock();
        try {
            return existingPaths.size();
        } finally {
            lock.readLock().unlock();
        }
    }

}
