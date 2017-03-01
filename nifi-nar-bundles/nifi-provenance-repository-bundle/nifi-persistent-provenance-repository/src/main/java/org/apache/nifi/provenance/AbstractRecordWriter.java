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

package org.apache.nifi.provenance;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.TocWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRecordWriter implements RecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRecordWriter.class);

    private final File file;
    private final String storageLocation;
    private final TocWriter tocWriter;
    private final Lock lock = new ReentrantLock();

    private volatile boolean dirty = false;
    private volatile boolean closed = false;

    public AbstractRecordWriter(final File file, final TocWriter writer) throws IOException {
        logger.trace("Creating Record Writer for {}", file);

        this.file = file;
        this.storageLocation = file.getName();
        this.tocWriter = writer;
    }

    public AbstractRecordWriter(final String storageLocation, final TocWriter writer) throws IOException {
        logger.trace("Creating Record Writer for {}", storageLocation);

        this.file = null;
        this.storageLocation = storageLocation;
        this.tocWriter = writer;
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;

        logger.trace("Closing Record Writer for {}", getStorageLocation());

        lock();
        try {
            flush();

            try {
                // We want to close 'out' only if the writer is not 'dirty'.
                // If the writer is dirty, then there was a failure to write
                // to disk, which means that we likely have a partial record written
                // to disk.
                //
                // If we call close() on out, it will in turn call flush() on the underlying
                // output stream, which is a BufferedOutputStream. As a result, we will end
                // up flushing the buffer after a partially written record, which results in
                // essentially random bytes being written to the repository, which causes
                // corruption and un-recoverability. Since we will close the underlying 'rawOutStream'
                // below, we will still appropriately clean up the resources help by this writer, so
                // we are still OK in terms of closing all resources held by the writer.
                final OutputStream buffered = getBufferedOutputStream();
                if (buffered != null && !isDirty()) {
                    buffered.close();
                }
            } finally {
                final OutputStream underlying = getUnderlyingOutputStream();
                if (underlying != null) {
                    try {
                        getUnderlyingOutputStream().close();
                    } finally {
                        if (tocWriter != null) {
                            tocWriter.close();
                        }
                    }
                }
            }
        } catch (final IOException ioe) {
            markDirty();
            throw ioe;
        } finally {
            unlock();
        }
    }

    protected String getStorageLocation() {
        return storageLocation;
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public boolean tryLock() {
        final boolean obtainedLock = lock.tryLock();
        if (obtainedLock && isDirty()) {
            // once we have obtained the lock, we need to check if the writer
            // has been marked dirty. If so, we cannot write to the underlying
            // file, so we need to unlock and return false. Otherwise, it's okay
            // to write to the underlying file, so return true.
            lock.unlock();
            return false;
        }
        return obtainedLock;
    }

    @Override
    public void markDirty() {
        this.dirty = true;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    protected void resetDirtyFlag() {
        this.dirty = false;
    }

    @Override
    public synchronized void sync() throws IOException {
        try {
            if (tocWriter != null) {
                tocWriter.sync();
            }

            syncUnderlyingOutputStream();
        } catch (final IOException ioe) {
            markDirty();
            throw ioe;
        }
    }

    @Override
    public TocWriter getTocWriter() {
        return tocWriter;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    protected abstract OutputStream getBufferedOutputStream();

    protected abstract OutputStream getUnderlyingOutputStream();

    protected abstract void syncUnderlyingOutputStream() throws IOException;
}
