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
package org.apache.nifi.provenance.journaling.tasks;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.io.StandardEventSerializer;
import org.apache.nifi.provenance.journaling.journals.JournalReader;
import org.apache.nifi.provenance.journaling.journals.JournalWriter;
import org.apache.nifi.provenance.journaling.journals.StandardJournalReader;
import org.apache.nifi.provenance.journaling.journals.StandardJournalWriter;
import org.apache.nifi.provenance.journaling.toc.StandardTocReader;
import org.apache.nifi.provenance.journaling.toc.StandardTocWriter;
import org.apache.nifi.provenance.journaling.toc.TocReader;
import org.apache.nifi.provenance.journaling.toc.TocWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressionTask implements Runnable {
    public static final String FILE_EXTENSION = ".compress";
    
    private static final Logger logger = LoggerFactory.getLogger(CompressionTask.class);
    
    private final File journalFile;
    private final long journalId;
    private final File tocFile;
    
    public CompressionTask(final File journalFile, final long journalId, final File tocFile) {
        this.journalFile = journalFile;
        this.journalId = journalId;
        this.tocFile = tocFile;
    }

    public void compress(final JournalReader reader, final JournalWriter writer, final TocReader tocReader, final TocWriter tocWriter) throws IOException {
        ProvenanceEventRecord event;
        
        int blockIndex = 0;
        long blockOffset = tocReader.getBlockOffset(blockIndex);
        tocWriter.addBlockOffset(blockOffset);
        long nextBlockOffset = tocReader.getBlockOffset(blockIndex + 1);
        
        try {
            while ((event = reader.nextEvent()) != null) {
                // Check if we've gone beyond the offset of the next block. If so, write
                // out a new block in the TOC.
                final long newPosition = reader.getPosition();
                if ( newPosition > nextBlockOffset && nextBlockOffset > 0 ) {
                    blockIndex++;
                    blockOffset = tocReader.getBlockOffset(blockIndex);
                    tocWriter.addBlockOffset(writer.getSize());
                    
                    nextBlockOffset = tocReader.getBlockOffset(blockIndex + 1);
                }
                
                // Write the event to the compressed writer
                writer.write(Collections.singleton(event), event.getEventId());
            }
        } catch (final EOFException eof) {
            logger.warn("Found unexpected End-of-File when compressing {}", reader);
        }
    }

    /**
     * Attempts to delete the given file up to 10 times, waiting a bit in between each failed
     * iteration, in case another process (for example, a virus scanner) has the file locked
     * 
     * @param file
     * @return
     */
    private boolean delete(final File file) {
        for (int i=0; i < 10; i++) {
            if ( file.delete() || !file.exists() ) {
                return true;
            }
            
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
            }
        }
        
        return false;
    }
    
    /**
     * Attempts to rename the given original file to the renamed file up to 20 times, waiting a bit
     * in between each failed iteration, in case another process (for example, a virus scanner) has 
     * the file locked
     * 
     * @param original
     * @param renamed
     * @return
     */
    public static boolean rename(final File original, final File renamed) {
        for (int i=0; i < 20; i++) {
            if ( original.renameTo(renamed) ) {
                return true;
            }
            
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
            }
        }
        
        return false;
    }
    
    @Override
    public void run() {
        try {
            final File compressedFile = new File(journalFile.getParentFile(), journalFile.getName() + FILE_EXTENSION);
            final File compressedTocFile = new File(tocFile.getParentFile(), tocFile.getName() + FILE_EXTENSION);

            try (final JournalReader journalReader = new StandardJournalReader(journalFile);
                final JournalWriter compressedWriter = new StandardJournalWriter(journalId, compressedFile, true, new StandardEventSerializer());
                final TocReader tocReader = new StandardTocReader(tocFile);
                final TocWriter compressedTocWriter = new StandardTocWriter(compressedTocFile, true)) {
                
                compress(journalReader, compressedWriter, tocReader, compressedTocWriter);
                compressedWriter.sync();
            }

            final boolean deletedJournal = delete(journalFile);
            if ( !deletedJournal ) {
                delete(compressedFile);
                delete(compressedTocFile);
                logger.error("Failed to remove Journal file {}; considering compression task a failure", journalFile);
                return;
            }
            
            final boolean deletedToc = delete(tocFile);
            if ( !deletedToc ) {
                delete(compressedFile);
                delete(compressedTocFile);
                logger.error("Failed to remove TOC file for {}; considering compression task a failure", journalFile);
                return;
            }
            
            final boolean renamedJournal = rename(compressedFile, journalFile);
            if ( !renamedJournal ) {
                logger.error("Failed to rename {} to {}; this journal file may be inaccessible until it is renamed", compressedFile, journalFile);
            }
            
            final boolean renamedToc = rename(compressedTocFile, tocFile);
            if ( !renamedToc ) {
                logger.error("Failed to rename {} to {}; this journal file may be inaccessible until it is renamed", compressedTocFile, tocFile);
            }
            
            logger.info("Successfully compressed Journal File {}");
        } catch (final IOException ioe) {
            logger.error("Failed to compress Journal File {} due to {}", journalFile, ioe.toString());
            if ( logger.isDebugEnabled() ) {
                logger.error("", ioe);
            }
        }
    }
    
}
