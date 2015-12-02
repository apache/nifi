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
package org.apache.nifi.provenance.toc;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard implementation of {@link TocWriter}.
 *
 * Format of .toc file:
 * byte 0: version
 * byte 1: compressed: 0 -> not compressed, 1 -> compressed
 * byte 2-9: long: offset of block 0
 * byte 10-17: long: offset of block 1
 * ...
 * byte (N*8+2)-(N*8+9): long: offset of block N
 */
public class StandardTocWriter implements TocWriter {
    private static final Logger logger = LoggerFactory.getLogger(StandardTocWriter.class);

    public static final byte VERSION = 2;

    private final File file;
    private final FileOutputStream fos;
    private final boolean alwaysSync;
    private int index = -1;

    /**
     * Creates a StandardTocWriter that writes to the given file.
     * @param file the file to write to
     * @param compressionFlag whether or not the journal is compressed
     * @throws IOException if unable to write header info to the specified file
     */
    public StandardTocWriter(final File file, final boolean compressionFlag, final boolean alwaysSync) throws IOException {
        final File tocDir = file.getParentFile();
        if ( !tocDir.exists() ) {
            Files.createDirectories(tocDir.toPath());
        }

        this.file = file;
        fos = new FileOutputStream(file);
        this.alwaysSync = alwaysSync;

        final byte[] header = new byte[2];
        header[0] = VERSION;
        header[1] = (byte) (compressionFlag ? 1 : 0);
        fos.write(header);
        fos.flush();

        if ( alwaysSync ) {
            sync();
        }
    }

    @Override
    public void addBlockOffset(final long offset, final long firstEventId) throws IOException {
        final BufferedOutputStream bos = new BufferedOutputStream(fos);
        final DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(offset);
        dos.writeLong(firstEventId);
        dos.flush();
        index++;
        logger.debug("Adding block {} at offset {}", index, offset);

        if ( alwaysSync ) {
            sync();
        }
    }

    @Override
    public void sync() throws IOException {
        fos.getFD().sync();
    }

    @Override
    public int getCurrentBlockIndex() {
        return index;
    }

    @Override
    public void close() throws IOException {
        if (alwaysSync) {
            fos.getFD().sync();
        }

        fos.close();
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public String toString() {
        return "TOC Writer for " + file;
    }
}
