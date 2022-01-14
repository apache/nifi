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
package org.apache.nifi.wali.pmem;

import org.apache.nifi.pmem.PmemMappedFile;
import org.apache.nifi.pmem.PmemMappedFile.PmemOutputStream;
import org.apache.nifi.wali.ByteArrayDataOutputStream;
import org.apache.nifi.wali.LengthDelimitedJournal;
import org.apache.nifi.wali.ObjectPool;
import org.apache.nifi.stream.io.SyncOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDeFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.apache.nifi.pmem.PmemMappedFile.PmemExtendStrategy.EXTEND_DOUBLY;
import static org.apache.nifi.pmem.PmemMappedFile.PmemPutStrategy.PUT_NO_FLUSH;

public class PmemLengthDelimitedJournal<T> extends LengthDelimitedJournal<T> {
    private static final Logger logger = LoggerFactory.getLogger(PmemLengthDelimitedJournal.class);
    private static final long DEFAULT_LIBPMEM_JOURNAL_BYTES = 128L * 1024 * 1024; // 128 MB
    private static final Set<PosixFilePermission> DEFAULT_MODE = PosixFilePermissions.fromString("rw-r--r--"); // 0644

    public PmemLengthDelimitedJournal(final File journalFile, final SerDeFactory<T> serdeFactory, final ObjectPool<ByteArrayDataOutputStream> streamPool, final long initialTransactionId) {
        super(journalFile, serdeFactory, streamPool, initialTransactionId);
    }

    public PmemLengthDelimitedJournal(final File journalFile, final SerDeFactory<T> serdeFactory, final ObjectPool<ByteArrayDataOutputStream> streamPool, final long initialTransactionId,
                                      final int maxInHeapSerializationBytes) {
        super(journalFile, serdeFactory, streamPool, initialTransactionId, maxInHeapSerializationBytes);
    }

    @Override
    protected SyncOutputStream newSyncOutputStream(File journalFile) throws FileNotFoundException {
        // DO NOT call super.newSyncOutputStream()

        try {
            final SyncPmemOutputStream out = new SyncPmemOutputStream(journalFile.getPath());
            logger.debug("PMEM created: {}", out.underlyingPmem().toString());
            return out;
        } catch (IOException e) {
            throw (FileNotFoundException) (new FileNotFoundException().initCause(e));
        }
    }

    private static class SyncPmemOutputStream extends SyncOutputStream {
        private final PmemOutputStream out;

        public SyncPmemOutputStream(String path) throws IOException {
            final PmemMappedFile pmem = PmemMappedFile.create(path, DEFAULT_LIBPMEM_JOURNAL_BYTES, DEFAULT_MODE);
            out = pmem.uniqueOutputStream(0L, EXTEND_DOUBLY, PUT_NO_FLUSH);
        }

        @Override
        public void write(int i) throws IOException {
            out.write(i);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void sync() throws IOException {
            out.sync();
        }

        @Override
        public void close() throws IOException {
            flush();
            sync();
            out.close();
            logger.debug("PMEM closed: {}", underlyingPmem().toString());
        }

        @Override
        public OutputStream getWrappedStream() {
            return out;
        }

        private PmemMappedFile underlyingPmem() {
            return out.underlyingPmem();
        }
    }
}
