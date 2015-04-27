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
package org.apache.nifi.provenance.serialization;

import java.io.Closeable;
import java.io.IOException;

import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.toc.TocReader;

public interface RecordReader extends Closeable {

	/**
	 * Returns the next record in the reader, or <code>null</code> if there is no more data available.
	 * @return
	 * @throws IOException
	 */
    StandardProvenanceEventRecord nextRecord() throws IOException;

    /**
     * Skips the specified number of bytes
     * @param bytesToSkip
     * @throws IOException
     */
    void skip(long bytesToSkip) throws IOException;

    /**
     * Skips to the specified byte offset in the underlying stream.
     * @param position
     * @throws IOException if the underlying stream throws IOException, or if the reader has already
     * passed the specified byte offset
     */
    void skipTo(long position) throws IOException;
    
    /**
     * Skips to the specified compression block
     * 
     * @param blockIndex
     * @throws IOException if the underlying stream throws IOException, or if the reader has already
     * read passed the specified compression block index
     * @throws IllegalStateException if the RecordReader does not have a TableOfContents associated with it
     */
    void skipToBlock(int blockIndex) throws IOException;
    
    /**
     * Returns the block index that the Reader is currently reading from.
     * Note that the block index is incremented at the beginning of the {@link #nextRecord()}
     * method. This means that this method will return the block from which the previous record was read, 
     * if calling {@link #nextRecord()} continually, not the block from which the next record will be read.
     * @return
     */
    int getBlockIndex();
    
    /**
     * Returns <code>true</code> if the compression block index is available. It will be available
     * if and only if the reader is created with a TableOfContents
     * 
     * @return
     */
    boolean isBlockIndexAvailable();
    
    /**
     * Returns the {@link TocReader} that is used to keep track of compression blocks, if one exists,
     * <code>null</code> otherwise
     * @return
     */
    TocReader getTocReader();
    
    /**
     * Returns the number of bytes that have been consumed from the stream (read or skipped).
     * @return
     */
    long getBytesConsumed();
    
    /**
     * Returns the ID of the last event in this record reader, or -1 if the reader has no records or
     * has already read through all records. Note: This method will consume the stream until the end,
     * so no more records will be available on this reader after calling this method.
     * 
     * @return
     * @throws IOException
     */
    long getMaxEventId() throws IOException;
}
