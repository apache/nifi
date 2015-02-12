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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.journals.JournalReader;
import org.apache.nifi.provenance.journaling.journals.StandardJournalReader;

public class TocJournalReader implements Closeable {

    private final TocReader tocReader;
    private final JournalReader reader;
    
    private final String containerName;
    private final String sectionName;
    private final String journalId;
    
    private int blockIndex;
    private long nextBlockOffset;
    
    
    public TocJournalReader(final String containerName, final String sectionName, final String journalId, final File journalFile) throws IOException {
        this.containerName = containerName;
        this.sectionName = sectionName;
        this.journalId = journalId;
        
        final File tocFile = new File(journalFile.getParentFile(), journalFile.getName() + ".toc");
        tocReader = new StandardTocReader(tocFile);
        reader = new StandardJournalReader(journalFile);
        
        blockIndex = 0;
        nextBlockOffset = tocReader.getBlockOffset(1);
    }
    
    @Override
    public void close() throws IOException {
        IOException suppressed = null;
        try {
            tocReader.close();
        } catch (final IOException ioe) {
            suppressed = ioe;
        }
        
        try {
            reader.close();
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
    
    public JournaledProvenanceEvent nextJournaledEvent() throws IOException {
        ProvenanceEventRecord event = reader.nextEvent();
        if ( event == null ) {
            return null;
        }
        
        final JournaledStorageLocation location = new JournaledStorageLocation(containerName, sectionName, 
                journalId, blockIndex, event.getEventId());
        
        // Check if we've gone beyond the offset of the next block. If so, write
        // out a new block in the TOC.
        final long newPosition = reader.getPosition();
        if ( newPosition > nextBlockOffset && nextBlockOffset > 0 ) {
            blockIndex++;
            nextBlockOffset = tocReader.getBlockOffset(blockIndex + 1);
        }
        
        return new JournaledProvenanceEvent(event, location);
    }
    
}
