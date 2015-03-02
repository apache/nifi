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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.io.StandardEventSerializer;
import org.apache.nifi.remote.io.CompressionOutputStream;
import org.junit.Before;
import org.junit.Test;

public class TestStandardJournalReader {

    private ByteArrayOutputStream baos;
    private DataOutputStream dos;
    
    @Before
    public void setup() throws IOException {
        // Create a BAOS to write the record to.
        baos = new ByteArrayOutputStream();
        dos = new DataOutputStream(baos);
        
        // Write out header: codec name and serialization version
        StandardJournalMagicHeader.write(dos);
        dos.writeUTF(StandardEventSerializer.CODEC_NAME);
        dos.writeInt(0);
    }
    
    
    @Test
    public void testReadFirstEventUncompressed() throws IOException {
        dos.writeBoolean(false);
        writeRecord(88L);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            final ProvenanceEventRecord restored = reader.nextEvent();
            assertNotNull(restored);
            assertEquals(88L, restored.getEventId());
            assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
            assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
        } finally {
            file.delete();
        }
    }
    
    
    @Test
    public void testReadManyUncompressed() throws IOException {
        dos.writeBoolean(false);
        writeRecords(0, 1024, false);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            for (int i=0; i < 1024; i++) {
                final ProvenanceEventRecord restored = reader.nextEvent();
                assertNotNull(restored);
                assertEquals((long) i, restored.getEventId());
                assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
                assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
            }
            
            assertNull(reader.nextEvent());
        } finally {
            file.delete();
        }
    }
    
    @Test
    public void testReadFirstEventWithBlockOffsetUncompressed() throws IOException {
        dos.writeBoolean(false);
        writeRecords(0, 10, false);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, false);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            final ProvenanceEventRecord restored = reader.getEvent(secondBlockOffset, 10L);
            assertNotNull(restored);
            assertEquals(10L, restored.getEventId());
            assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
            assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
        } finally {
            file.delete();
        }
    }
    
    @Test
    public void testReadSubsequentEventWithBlockOffsetUncompressed() throws IOException {
        dos.writeBoolean(false);
        writeRecords(0, 10, false);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, false);

        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            final ProvenanceEventRecord restored = reader.getEvent(secondBlockOffset, 10L);
            assertNotNull(restored);
            assertEquals(10L, restored.getEventId());
            assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
            assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
        } finally {
            file.delete();
        }
    }
    
    @Test
    public void testReadMultipleEventsWithBlockOffsetUncompressed() throws IOException {
        dos.writeBoolean(false);
        writeRecords(0, 10, false);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, false);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            for (int i=0; i < 2; i++) {
                final ProvenanceEventRecord event10 = reader.getEvent(secondBlockOffset, 10L);
                assertNotNull(event10);
                assertEquals(10L, event10.getEventId());
                assertEquals(ProvenanceEventType.CREATE, event10.getEventType());
                assertEquals("00000000-0000-0000-0000-000000000000", event10.getFlowFileUuid());
                
                final ProvenanceEventRecord event13 = reader.getEvent(secondBlockOffset, 13L);
                assertNotNull(event13);
                assertEquals(13L, event13.getEventId());
                assertEquals(ProvenanceEventType.CREATE, event13.getEventType());
                assertEquals("00000000-0000-0000-0000-000000000000", event13.getFlowFileUuid());
            }
        } finally {
            file.delete();
        }
    }
    
    
    @Test
    public void testReadFirstEventCompressed() throws IOException {
        dos.writeBoolean(true);
        writeRecords(88L, 1, true);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            final ProvenanceEventRecord restored = reader.nextEvent();
            assertNotNull(restored);
            assertEquals(88L, restored.getEventId());
            assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
            assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
        } finally {
            file.delete();
        }
    }
    
    @Test
    public void testReadManyCompressed() throws IOException {
        dos.writeBoolean(true);
        writeRecords(0, 1024, true);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            for (int i=0; i < 1024; i++) {
                final ProvenanceEventRecord restored = reader.nextEvent();
                assertNotNull(restored);
                assertEquals((long) i, restored.getEventId());
                assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
                assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
            }
            
            assertNull(reader.nextEvent());
        } finally {
            file.delete();
        }
    }
    
    
    @Test
    public void testReadFirstEventWithBlockOffsetCompressed() throws IOException {
        dos.writeBoolean(true);
        writeRecords(0, 10, true);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, true);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            final ProvenanceEventRecord restored = reader.getEvent(secondBlockOffset, 10L);
            assertNotNull(restored);
            assertEquals(10L, restored.getEventId());
            assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
            assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
        } finally {
            file.delete();
        }
    }
    
    @Test
    public void testReadSubsequentEventWithBlockOffsetCompressed() throws IOException {
        dos.writeBoolean(true);
        writeRecords(0, 10, true);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, true);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            final ProvenanceEventRecord restored = reader.getEvent(secondBlockOffset, 10L);
            assertNotNull(restored);
            assertEquals(10L, restored.getEventId());
            assertEquals(ProvenanceEventType.CREATE, restored.getEventType());
            assertEquals("00000000-0000-0000-0000-000000000000", restored.getFlowFileUuid());
        } finally {
            file.delete();
        }
    }
    
    @Test
    public void testReadMultipleEventsWithBlockOffsetCompressed() throws IOException {
        dos.writeBoolean(true);
        writeRecords(0, 10, true);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, true);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            for (int i=0; i < 2; i++) {
                final ProvenanceEventRecord event10 = reader.getEvent(secondBlockOffset, 10L);
                assertNotNull(event10);
                assertEquals(10L, event10.getEventId());
                assertEquals(ProvenanceEventType.CREATE, event10.getEventType());
                assertEquals("00000000-0000-0000-0000-000000000000", event10.getFlowFileUuid());
                
                final ProvenanceEventRecord event13 = reader.getEvent(secondBlockOffset, 13L);
                assertNotNull(event13);
                assertEquals(13L, event13.getEventId());
                assertEquals(ProvenanceEventType.CREATE, event13.getEventType());
                assertEquals("00000000-0000-0000-0000-000000000000", event13.getFlowFileUuid());
            }
        } finally {
            file.delete();
        }
    }
    
    
    @Test
    public void testReadEventWithBlockOffsetThenPreviousBlockOffsetUncompressed() throws IOException {
        dos.writeBoolean(false);
        final int firstBlockOffset = baos.size();
        writeRecords(0, 10, false);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, false);

        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            for (int j=0; j < 2; j++) {
                for (int i=0; i < 2; i++) {
                    final ProvenanceEventRecord event10 = reader.getEvent(secondBlockOffset, 10L);
                    assertNotNull(event10);
                    assertEquals(10L, event10.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event10.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event10.getFlowFileUuid());
                    
                    final ProvenanceEventRecord event13 = reader.getEvent(secondBlockOffset, 13L);
                    assertNotNull(event13);
                    assertEquals(13L, event13.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event13.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event13.getFlowFileUuid());
                }
                
                for (int i=0; i < 2; i++) {
                    final ProvenanceEventRecord event2 = reader.getEvent(firstBlockOffset, 2L);
                    assertNotNull(event2);
                    assertEquals(2L, event2.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event2.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event2.getFlowFileUuid());
                    
                    final ProvenanceEventRecord event6 = reader.getEvent(firstBlockOffset, 6L);
                    assertNotNull(event6);
                    assertEquals(6L, event6.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event6.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event6.getFlowFileUuid());
                }
            }
        } finally {
            file.delete();
        }
    }
    
    
    @Test
    public void testReadEventWithBlockOffsetThenPreviousBlockOffsetCompressed() throws IOException {
        dos.writeBoolean(true);
        final int firstBlockOffset = baos.size();
        writeRecords(0, 10, true);
        
        final int secondBlockOffset = baos.size();
        writeRecords(10, 10, true);
        
        // write data to a file so that we can read it with the journal reader
        final File dir = new File("target/testData");
        final File file = new File(dir, UUID.randomUUID().toString() + ".journal");
        dir.mkdirs();
        
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            baos.writeTo(fos);
        }
        
        // read the record and verify its contents
        try (final StandardJournalReader reader = new StandardJournalReader(file)) {
            for (int j=0; j < 2; j++) {
                for (int i=0; i < 2; i++) {
                    final ProvenanceEventRecord event10 = reader.getEvent(secondBlockOffset, 10L);
                    assertNotNull(event10);
                    assertEquals(10L, event10.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event10.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event10.getFlowFileUuid());
                    
                    final ProvenanceEventRecord event13 = reader.getEvent(secondBlockOffset, 13L);
                    assertNotNull(event13);
                    assertEquals(13L, event13.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event13.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event13.getFlowFileUuid());
                }
                
                for (int i=0; i < 2; i++) {
                    final ProvenanceEventRecord event2 = reader.getEvent(firstBlockOffset, 2L);
                    assertNotNull(event2);
                    assertEquals(2L, event2.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event2.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event2.getFlowFileUuid());
                    
                    final ProvenanceEventRecord event6 = reader.getEvent(firstBlockOffset, 6L);
                    assertNotNull(event6);
                    assertEquals(6L, event6.getEventId());
                    assertEquals(ProvenanceEventType.CREATE, event6.getEventType());
                    assertEquals("00000000-0000-0000-0000-000000000000", event6.getFlowFileUuid());
                }
            }
        } finally {
            file.delete();
        }
    }
    

    
    
    private void writeRecord(final long id) throws IOException {
        writeRecord(id, dos);
    }
    
    private void writeRecords(final long startId, final int numRecords, final boolean compressed) throws IOException {
        if ( compressed ) {
            final CompressionOutputStream compressedOut = new CompressionOutputStream(dos);
            for (long id = startId; id < startId + numRecords; id++) {
                writeRecord(id, new DataOutputStream(compressedOut));
            }
            compressedOut.close();
        } else {
            for (long id = startId; id < startId + numRecords; id++) {
                writeRecord(id, dos);
            }
        }
    }
    
    private void writeRecord(final long id, final DataOutputStream dos) throws IOException {
        // Create prov event to add to the stream
        final ProvenanceEventRecord event = new StandardProvenanceEventRecord.Builder()
            .setEventType(ProvenanceEventType.CREATE)
            .setFlowFileUUID("00000000-0000-0000-0000-000000000000")
            .setComponentType("Unit Test")
            .setComponentId(UUID.randomUUID().toString())
            .setEventTime(System.currentTimeMillis())
            .setFlowFileEntryDate(System.currentTimeMillis() - 1000L)
            .setLineageStartDate(System.currentTimeMillis() - 2000L)
            .setCurrentContentClaim(null, null, null, null, 0L)
            .build();
        
        // Serialize the prov event
        final ByteArrayOutputStream serializationStream = new ByteArrayOutputStream();
        final StandardEventSerializer serializer = new StandardEventSerializer();
        serializer.serialize(event, new DataOutputStream(serializationStream));
        
        // Write out to our stream the event length, followed by the id, and then the serialized event
        final int recordLen = 8 + serializationStream.size();
        
        dos.writeInt(recordLen);
        dos.writeLong(id);
        serializationStream.writeTo(dos);
    }
}
