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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.TestUtil;
import org.apache.nifi.provenance.journaling.io.StandardEventSerializer;
import org.junit.Assert;
import org.junit.Test;

public class TestJournalReadWrite {

    @Test
    public void testReadWrite100Blocks() throws IOException {
        testReadWrite100Blocks(true);
        testReadWrite100Blocks(false);
    }
    
    private void testReadWrite100Blocks(final boolean compressed) throws IOException {
        final long journalId = 1L;
        final File journalFile = new File("target/1.journal");
        final StandardEventSerializer serializer = new StandardEventSerializer();
        
        try {
            final CompressionCodec codec = compressed ? new DeflatorCompressionCodec() : null;
            try (final StandardJournalWriter writer = new StandardJournalWriter(journalId, journalFile, codec, serializer)) {
                for (int block=0; block < 100; block++) {
                    writer.beginNewBlock();
                    
                    for (int i=0; i < 5; i++) {
                        final ProvenanceEventRecord event = TestUtil.generateEvent(i);
                        writer.write(Collections.singleton(event), i);
                    }
                    
                    final List<ProvenanceEventRecord> events = new ArrayList<>();
                    for (int i=0; i < 90; i++) {
                        events.add(TestUtil.generateEvent(i + 5));
                    }
                    writer.write(events, 5);
                    
                    for (int i=0; i < 5; i++) {
                        final ProvenanceEventRecord event = TestUtil.generateEvent(i);
                        writer.write(Collections.singleton(event), 95 + i);
                    }
                    
                    writer.finishBlock();
                }
            }
            
            try (final StandardJournalReader reader = new StandardJournalReader(journalFile)) {
                for (int block=0; block < 100; block++) {
                    for (int i=0; i < 100; i++) {
                        final ProvenanceEventRecord record = reader.nextEvent();
                        Assert.assertNotNull(record);
                        Assert.assertEquals((long) i, record.getEventId());
                    }
                }
            }
        } finally {
            journalFile.delete();
        }
    }
}
