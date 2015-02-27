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
package org.apache.nifi.provenance.journaling.toc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;

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
    public static final byte VERSION = 1;
    
    private final File file;
    private final FileOutputStream fos;
    private final boolean alwaysSync;
    private int index = 0;
    
    /**
     * Creates a StandardTocWriter that writes to the given file.
     * @param file the file to write to
     * @param compressionFlag whether or not the journal is compressed
     * @throws FileNotFoundException 
     */
    public StandardTocWriter(final File file, final boolean compressionFlag, final boolean alwaysSync) throws IOException {
        if ( file.exists() ) {
            // Check if the header actually exists. If so, throw FileAlreadyExistsException
            // If no data is in the file, we will just overwrite it.
            try (final InputStream fis = new FileInputStream(file);
                 final InputStream bis = new BufferedInputStream(fis);
                 final DataInputStream dis = new DataInputStream(bis)) {
                dis.read();
                dis.read();

                // we always add the first offset when the writer is created so we allow this to exist.
                dis.readLong();
                final int nextByte = dis.read();
                
                if ( nextByte > -1 ) {
                    throw new FileAlreadyExistsException(file.getAbsolutePath());
                }
            } catch (final EOFException eof) {
                // no real data. overwrite file.
            }
        }
        
        if ( !file.getParentFile().exists() && !file.getParentFile().mkdirs() ) {
            throw new IOException("Could not create directory " + file.getParent());
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
            fos.getFD().sync();
        }
    }
    
    @Override
    public void addBlockOffset(final long offset) throws IOException {
        final BufferedOutputStream bos = new BufferedOutputStream(fos);
        final DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(offset);
        dos.flush();
        
        if ( alwaysSync ) {
            fos.getFD().sync();
        }
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
