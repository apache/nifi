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
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.io.Deserializer;
import org.apache.nifi.provenance.journaling.io.Deserializers;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.MinimumLengthInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard implementation of {@link JournalReader}. This reader reads data that is written
 * in the format specified by {@link StandardJournalWriter}
 */
public class StandardJournalReader implements JournalReader {
    private static final Logger logger = LoggerFactory.getLogger(StandardJournalReader.class);
    
    private final File file;
    
    private ByteCountingInputStream compressedStream;
    private ByteCountingInputStream decompressedStream;
    
    private Deserializer deserializer;
    private int serializationVersion;
    private CompressionCodec compressionCodec = null;
    
    private long lastEventIdRead = -1L;
    
    
    public StandardJournalReader(final File file) throws IOException {
        this.file = file;
        resetStreams();
    }
    
    private void resetStreams() throws IOException {
        final InputStream bufferedIn = new BufferedInputStream(new FileInputStream(file));
        compressedStream = new ByteCountingInputStream(bufferedIn);
        try {
            final DataInputStream dis = new DataInputStream(compressedStream);
            
            StandardJournalMagicHeader.read(dis);
            final String codecName = dis.readUTF();
            serializationVersion = dis.readInt();
            final boolean compressed = dis.readBoolean();
            if ( compressed ) {
                final String compressionCodecName = dis.readUTF();
                if ( DeflatorCompressionCodec.DEFLATOR_COMPRESSION_CODEC.equals(compressionCodecName) ) {
                    compressionCodec = new DeflatorCompressionCodec();
                } else {
                    throw new IOException(file + " is compressed using unknown Compression Codec " + compressionCodecName);
                }
            }
            deserializer = Deserializers.getDeserializer(codecName);
            
            resetDecompressedStream();
        } catch (final Exception e) {
            try {
                compressedStream.close();
            } catch (final IOException ignore) {}
            
            throw new IOException("Failed to reset data stream when reading" + file, e);
        }
    }
    
    
    private void resetDecompressedStream() throws IOException {
        if ( compressionCodec == null ) {
            decompressedStream = compressedStream;
        } else {
            decompressedStream = new ByteCountingInputStream(new BufferedInputStream(compressionCodec.newCompressionInputStream(compressedStream)), compressedStream.getBytesConsumed());
        }
    }
    
    @Override
    public void close() throws IOException {
        compressedStream.close();
        
        if ( compressedStream != decompressedStream ) {
            decompressedStream.close();
        }
    }
    
    @Override
    public ProvenanceEventRecord nextEvent() throws IOException {
        return nextEvent(true);
    }
    
    @Override
    public long getPosition() {
        return decompressedStream.getBytesConsumed();
    }
    
    private boolean isData(final InputStream in) throws IOException {
        in.mark(1);
        final int b = in.read();
        if ( b < 0 ) {
            return false;
        }
        in.reset();
        
        return true;
    }
    
    ProvenanceEventRecord nextEvent(final boolean spanBlocks) throws IOException {
        boolean isData = isData(decompressedStream);
        if ( !isData ) {
            if ( !spanBlocks ) {
                return null;
            }
            
            // we are allowed to span blocks. We're out of data but if we are compressed, it could
            // just mean that the block has ended.
            if ( compressionCodec == null ) {
                return null;
            }
            
            isData = isData(compressedStream);
            if ( !isData ) {
                return null;
            }
            
            // There is no data in the compressed InputStream but there is in the underlying stream.
            // This means we've hit the end of our block. We will create a new CompressionInputStream
            // so that we can continue reading.
            resetDecompressedStream();
        }
        
        try {
            final DataInputStream dis = new DataInputStream(decompressedStream);
            final int eventLength = dis.readInt();
            
            final LimitingInputStream limitingInputStream = new LimitingInputStream(dis, eventLength);
            final MinimumLengthInputStream minStream = new MinimumLengthInputStream(limitingInputStream, eventLength);
            final ProvenanceEventRecord event = deserializer.deserialize(new DataInputStream(minStream), serializationVersion);
            lastEventIdRead = event.getEventId();
            return event;
        } catch (final EOFException eof) {
            logger.warn("{} Found unexpected End-of-File when reading from journal", this); 
            return null;
        }
    }
    
    @Override
    public ProvenanceEventRecord getEvent(final long blockOffset, final long eventId) throws IOException {
        // If the requested event ID is less than the last event that we read, we need to reset to the beginning
        // of the file. We do this because we know that the ID's are always increasing, so if we need an ID less
        // than the previous ID, we have to go backward in the file. We can't do this with streams, so start the
        // stream over.
        if ( eventId <= lastEventIdRead ) {
            close();
            resetStreams();
        }
        
        final long bytesToSkip = blockOffset - compressedStream.getBytesConsumed();
        if ( bytesToSkip > 0 ) {
            StreamUtils.skip(compressedStream, bytesToSkip);
            resetDecompressedStream();
        }
        
        ProvenanceEventRecord event;
        while ((event = nextEvent()) != null) {
            if ( event.getEventId() == eventId ) {
                return event;
            }
        }
        
        throw new IOException("Could not find event with ID " + eventId + " in " + this);
    }

    @Override
    public String toString() {
        return "StandardJournalReader[" + file + "]";
    }

    @Override
    public ProvenanceEventRecord getLastEvent(final long blockOffset) throws IOException {
        if ( blockOffset > compressedStream.getBytesConsumed() ) {
            close();
            resetStreams();
        }
        
        final long bytesToSkip = blockOffset - compressedStream.getBytesConsumed();
        if ( bytesToSkip > 0 ) {
            StreamUtils.skip(compressedStream, bytesToSkip);
            resetDecompressedStream();
        }
        
        ProvenanceEventRecord lastReadRecord = null;
        ProvenanceEventRecord event;
        while ((event = nextEvent()) != null) {
            lastReadRecord = event;
        }
        
        // If we weren't able to read anything and the block offset was given, just start over
        // and read the entire thing, returning the last event.
        if ( lastReadRecord == null && blockOffset > 0L ) {
            return getLastEvent(0L);
        }
        
        // return the last even that we read, whether or not it was null
        return lastReadRecord;
    }
}
