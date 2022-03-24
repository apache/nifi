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
package org.apache.nifi.util;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.nifi.controller.queue.QueueSize;


public class MockFlowFileQueue {

    private final BlockingQueue<MockFlowFile> queue;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public MockFlowFileQueue() {
        queue = new LinkedBlockingQueue<>();
    }

    public void offer(final MockFlowFile flowFile) {
        writeLock.lock();
        try {
            queue.offer(flowFile);
        } finally {
            writeLock.unlock();
        }
    }

    public MockFlowFile poll() {
        writeLock.lock();
        try {
            return queue.poll();
        } finally {
            writeLock.unlock();
        }
    }

    public void addAll(final Collection<MockFlowFile> flowFiles) {
        writeLock.lock();
        try {
            queue.addAll(flowFiles);
        } finally {
            writeLock.unlock();
        }
    }

    public QueueSize size() {
        readLock.lock();
        try {
            final int count = queue.size();

            long contentSize = 0L;
            for (final MockFlowFile flowFile : queue) {
                contentSize += flowFile.getSize();
            }
            return new QueueSize(count, contentSize);
        } finally {
            readLock.unlock();
        }
    }

    public boolean isEmpty() {
        return size().getObjectCount() == 0;
    }
}
