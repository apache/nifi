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
package org.apache.nifi.provenance.journaling.journals;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.io.Serializer;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Standard implementation of {@link JournalWriter}.
 * </p>
 * 
 * <p>
 * Writes out to a journal file using the format:
 * 
 * <pre>
 * &lt;header&gt;
 * &lt;begin block 1&gt;
 * &lt;record 1&gt;
 * &lt;record 2&gt;
 * &lt;record 3&gt;
 * &lt;end block 1&gt;
 * &lt;begin block 2&gt;
 * &lt;record 4&gt;
 * &lt;record 5&gt;
 * &lt;end block 2&gt;
 * ...
 * &lt;begin block N&gt;
 * &lt;record N&gt;
 * &lt;end block N&gt;
 * </pre>
 * 
 * Where &lt;header&gt; is defined as:
 * <pre>
 *  magic header "NiFiProvJournal_1"
 *  String: serialization codec name (retrieved from serializer)
 *      --> 2 bytes for length of string
 *      --> N bytes for actual serialization codec name
 *  int: serialization version
 *  boolean: compressed: 1 -> compressed, 0 -> not compressed
 *  String : if compressed, name of compression codec; otherwise, not present
 * </pre>
 * 
 * And &lt;record&gt; is defined as:
 * <pre>
 * bytes 0-3: int: record length
 * bytes 4-11: long: record id
 * bytes 12-N: serialized event according to the applied {@link Serializer}
 * </pre>
 * </p>
 * 
 * <p>
 * The structure of the &lt;begin block&gt; and &lt;end block&gt; element depend on whether or not
 * compression is enabled. If the journal is not compressed, these elements are 0 bytes.
 * If the journal is compressed, these are the compression header and compression footer, respectively.
 * </p>
 * 
 */
public class StandardJournalWriter implements JournalWriter {
    private static final Logger logger = LoggerFactory.getLogger(StandardJournalWriter.class);
    
    private final long journalId;
    private final File journalFile;
    private final CompressionCodec compressionCodec;
    private final Serializer serializer;
    private final long creationTime = System.nanoTime();
    private final String description;
    
    private int eventCount;
    private boolean blockStarted = false;
    
    private final FileOutputStream fos;
    private ByteCountingOutputStream uncompressedStream;
    private OutputStream compressedStream;
    private ByteCountingOutputStream out;
    
    private long recordBytes = 256L;
    private long recordCount = 1L;
    
    
    public StandardJournalWriter(final long journalId, final File journalFile, final CompressionCodec compressionCodec, final Serializer serializer) throws IOException {
        if ( journalFile.exists() ) {
            // Check if there is actually any data here.
            try (final InputStream fis = new FileInputStream(journalFile);
                 final InputStream bufferedIn = new BufferedInputStream(fis);
                 final DataInputStream dis = new DataInputStream(bufferedIn) ) {
                
                StandardJournalMagicHeader.read(dis);
                dis.readUTF();
                dis.readInt();
                dis.readBoolean();
                final int nextByte = dis.read();
                if ( nextByte > -1 ) {
                    throw new FileAlreadyExistsException(journalFile.getAbsolutePath());
                }
            } catch (final EOFException eof) {
                // If we catch an EOF, there's no real data here, so we can overwrite the file.
            }
        }
        
        this.journalId = journalId;
        this.journalFile = journalFile;
        this.compressionCodec = compressionCodec;
        this.serializer = serializer;
        this.description = "Journal Writer for " + journalFile;
        this.fos = new FileOutputStream(journalFile);
        
        uncompressedStream = new ByteCountingOutputStream(fos);
        writeHeader(uncompressedStream);
        
        if (compressionCodec != null) {
            final CompressedOutputStream cos = compressionCodec.newCompressionOutputStream(uncompressedStream);
            cos.beginNewBlock();
            compressedStream = cos;
        } else {
            compressedStream = fos;
        }
        
        this.out = new ByteCountingOutputStream(compressedStream, uncompressedStream.getBytesWritten());
    }

    private void writeHeader(final OutputStream out) throws IOException {
        final DataOutputStream dos = new DataOutputStream(out);
        StandardJournalMagicHeader.write(out);
        dos.writeUTF(serializer.getCodecName());
        dos.writeInt(serializer.getVersion());
        
        final boolean compressed = compressionCodec != null;
        dos.writeBoolean(compressed);
        if ( compressed ) {
            dos.writeUTF(compressionCodec.getName());
        }
        
        dos.flush();
    }
    
    @Override
    public long getJournalId() {
        return journalId;
    }
    
    @Override
    public void close() throws IOException {
        finishBlock();

        IOException suppressed = null;
        try {
            compressedStream.flush();
            compressedStream.close();
        } catch (final IOException ioe) {
            suppressed = ioe;
        }
        
        try {
            try {
                uncompressedStream.flush();
            } finally {
                uncompressedStream.close();
            }
        } catch (final IOException ioe) {
            if ( suppressed != null ) {
                ioe.addSuppressed(suppressed);
            }
            throw ioe;
        }
        
        if ( suppressed != null ) {
            throw suppressed;
        }
    }

    @Override
    public void write(final Collection<ProvenanceEventRecord> events, final long firstEventId) throws IOException {
        final long start = System.nanoTime();
        final int avgRecordSize = (int) (recordBytes / recordCount);
        
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(avgRecordSize);
        final DataOutputStream serializerDos = new DataOutputStream(baos);
        
        final BufferedOutputStream bos = new BufferedOutputStream(out);
        final DataOutputStream outDos = new DataOutputStream(bos);
        
        try {
            long id = firstEventId;
            for ( final ProvenanceEventRecord event : events ) {
                serializer.serialize(event, serializerDos);
                serializerDos.flush();
                
                final int serializedLength = baos.size();
                final int recordLength = 8 + serializedLength;   // record length is length of ID (8 bytes) plus length of serialized record
                outDos.writeInt(recordLength);
                outDos.writeLong(id++);
                baos.writeTo(outDos);
                recordBytes += recordLength;
                recordCount++;
                baos.reset();
                
                eventCount++;
            }
        } finally {
            outDos.flush();
        }
        
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Finished writing {} events to {} in {} millis", events.size(), this, millis);
    }
    

    @Override
    public File getJournalFile() {
        return journalFile;
    }

    @Override
    public void sync() throws IOException {
        final long start = System.nanoTime();
        fos.getFD().sync();
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()- start);
        logger.debug("Successfully sync'ed {} in {} millis", this, millis);
    }

    @Override
    public long getSize() {
        return out.getBytesWritten();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    @Override
    public long getAge(final TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime() - creationTime, TimeUnit.NANOSECONDS);
    }
    

    @Override
    public void finishBlock() throws IOException {
        if ( !blockStarted ) {
            return;
        }
        
        blockStarted = false;
        
        if ( compressedStream instanceof CompressedOutputStream ) {
            ((CompressedOutputStream) compressedStream).finishBlock();
        }
    }
    
    @Override
    public void beginNewBlock() throws IOException {
        if ( blockStarted ) {
            throw new IllegalStateException("Block is already started");
        }
        blockStarted = true;
        
        if ( compressedStream instanceof CompressedOutputStream ) {
            ((CompressedOutputStream) compressedStream).beginNewBlock();
            this.out = new ByteCountingOutputStream(compressedStream, uncompressedStream.getBytesWritten());
        }
    }
    
    @Override
    public String toString() {
        return description;
    }
}
