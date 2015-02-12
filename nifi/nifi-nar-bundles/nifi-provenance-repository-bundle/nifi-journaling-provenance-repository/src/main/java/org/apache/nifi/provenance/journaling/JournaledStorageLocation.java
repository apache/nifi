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
package org.apache.nifi.provenance.journaling;

import java.util.Comparator;

import org.apache.nifi.provenance.StorageLocation;

public class JournaledStorageLocation implements StorageLocation, Comparable<JournaledStorageLocation>, Comparator<JournaledStorageLocation> {
    private final String containerName;
    private final String sectionName;
    private final String journalId;
    private final int blockIndex;
    private final long eventId;
    
    public JournaledStorageLocation(final String containerName, final String sectionName, final String journalId, final int blockIndex, final long eventId) {
        this.containerName = containerName;
        this.sectionName = sectionName;
        this.journalId = journalId;
        this.blockIndex = blockIndex;
        this.eventId = eventId;
    }
    
    public String getContainerName() {
        return containerName;
    }
    
    public String getSectionName() {
        return sectionName;
    }
    
    public String getJournalId() {
        return journalId;
    }
    
    public int getBlockIndex() {
        return blockIndex;
    }
    
    public long getEventId() {
        return eventId;
    }

    @Override
    public int compare(final JournaledStorageLocation o1, final JournaledStorageLocation o2) {
        final int containerVal = o1.getContainerName().compareTo(o2.getContainerName());
        if ( containerVal != 0 ) {
            return containerVal;
        }
        
        final int sectionVal = o1.getSectionName().compareTo(o2.getSectionName());
        if ( sectionVal != 0 ) {
            return sectionVal;
        }
        
        final int journalVal = o1.getJournalId().compareTo(o2.getJournalId());
        if ( journalVal != 0 ) {
            return journalVal;
        }
        
        final int blockVal = Integer.compare(o1.getBlockIndex(), o2.getBlockIndex());
        if ( blockVal != 0 ) {
            return blockVal;
        }
        
        return Long.compare(o1.getEventId(), o2.getEventId());
    }

    @Override
    public int compareTo(final JournaledStorageLocation o) {
        return compare(this, o);
    }
}
