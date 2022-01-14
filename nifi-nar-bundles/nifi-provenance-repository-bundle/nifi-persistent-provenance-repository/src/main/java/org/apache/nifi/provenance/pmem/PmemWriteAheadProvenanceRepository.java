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
package org.apache.nifi.provenance.pmem;

import org.apache.nifi.pmem.PmemMappedFile;
import org.apache.nifi.pmem.PmemMappedFile.PmemExtendStrategy;
import org.apache.nifi.pmem.PmemMappedFile.PmemOutputStream;
import org.apache.nifi.provenance.EventIdFirstSchemaRecordWriter;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.WriteAheadProvenanceRepository;
import org.apache.nifi.provenance.store.RecordWriterFactory;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.stream.io.SyncOutputStream;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.apache.nifi.pmem.PmemMappedFile.PmemExtendStrategy.EXTEND_DOUBLY;
import static org.apache.nifi.pmem.PmemMappedFile.PmemExtendStrategy.EXTEND_TO_FIT;
import static org.apache.nifi.pmem.PmemMappedFile.PmemPutStrategy.PUT_NO_FLUSH;

public class PmemWriteAheadProvenanceRepository extends WriteAheadProvenanceRepository {
    private static final Logger logger = LoggerFactory.getLogger(PmemWriteAheadProvenanceRepository.class);
    private static final Set<PosixFilePermission> DEFAULT_MODE = PosixFilePermissions.fromString("rw-r--r--"); // 0644

    public PmemWriteAheadProvenanceRepository() {
        super();
    }

    public PmemWriteAheadProvenanceRepository(final NiFiProperties nifiProperties) {
        super(nifiProperties);
    }

    public PmemWriteAheadProvenanceRepository(final RepositoryConfiguration config) {
        super(config);
    }

    @Override
    protected RecordWriterFactory newRecordWriterFactory(final long maxEventFileCapacity, final int uncompressedBlockSize, final IdentifierLookup idLookup) {
        // DO NOT call super.newRecordWriterFactory()

        return (file, idGenerator, compressed, createToc) -> {
            final TocWriter tocWriter = (createToc)
                    ? new StandardTocWriter(
                            (tocFile) -> new SyncPmemOutputStream(tocFile, maxEventFileCapacity, EXTEND_DOUBLY),
                            TocUtil.getTocFile(file), false, false)
                    : null;
            return new EventIdFirstSchemaRecordWriter(
                    (provFile) -> new SyncPmemOutputStream(provFile, maxEventFileCapacity, EXTEND_TO_FIT),
                    file, idGenerator, tocWriter, compressed, uncompressedBlockSize, idLookup);
        };
    }

    private static class SyncPmemOutputStream extends SyncOutputStream {
        private final PmemOutputStream out;

        public SyncPmemOutputStream(final File file, final long capacity, final PmemExtendStrategy strategy) throws IOException {
            final PmemMappedFile pmem = PmemMappedFile.create(file.getPath(), capacity, DEFAULT_MODE);
            out = pmem.uniqueOutputStream(0L, strategy, PUT_NO_FLUSH);
            logger.debug("PMEM created: {}", pmem.toString());
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
            logger.debug("PMEM drained: {}", out.underlyingPmem().toString());
        }

        @Override
        public void close() throws IOException {
            out.close();
            logger.debug("PMEM closed: {}", out.underlyingPmem().toString());
        }

        @Override
        public OutputStream getWrappedStream() {
            return out;
        }
    }
}
