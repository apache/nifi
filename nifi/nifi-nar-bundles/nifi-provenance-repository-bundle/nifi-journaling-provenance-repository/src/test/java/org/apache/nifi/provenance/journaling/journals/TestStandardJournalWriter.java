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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.UUID;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.journaling.TestUtil;
import org.apache.nifi.provenance.journaling.io.StandardEventDeserializer;
import org.apache.nifi.provenance.journaling.io.StandardEventSerializer;
import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.util.file.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestStandardJournalWriter {

    @Test
    public void testOverwriteEmptyFile() throws IOException {
        final File journalFile = new File("target/" + UUID.randomUUID().toString());
        try {
            assertTrue( journalFile.createNewFile() );
            
            try (final StandardJournalWriter writer = new StandardJournalWriter(1L, journalFile, true, new StandardEventSerializer())) {
                
            }
        } finally {
            FileUtils.deleteFile(journalFile, false);
        }
    }
    
    @Test
    public void testDoNotOverwriteNonEmptyFile() throws IOException {
        final File journalFile = new File("target/" + UUID.randomUUID().toString());
        try {
            assertTrue( journalFile.createNewFile() );
            
            try (final StandardJournalWriter writer = new StandardJournalWriter(1L, journalFile, true, new StandardEventSerializer())) {
                writer.write(Collections.singleton(TestUtil.generateEvent(1L)), 1L);
            }
            
            try (final StandardJournalWriter writer = new StandardJournalWriter(1L, journalFile, true, new StandardEventSerializer())) {
                Assert.fail("StandardJournalWriter attempted to overwrite existing file");
            } catch (final FileAlreadyExistsException faee) {
                // expected
            }
        } finally {
            FileUtils.deleteFile(journalFile, false);
        }
    }
    
    @Test
    public void testOneBlockOneRecordWriteCompressed() throws IOException {
        final File journalFile = new File("target/" + UUID.randomUUID().toString());
        
        final StandardEventSerializer serializer = new StandardEventSerializer();
        try {
            try (final StandardJournalWriter writer = new StandardJournalWriter(1L, journalFile, true, serializer)) {
                writer.beginNewBlock();
                writer.write(Collections.singleton(TestUtil.generateEvent(1L)), 1L);
                writer.finishBlock();
            }
        
            final byte[] data = Files.readAllBytes(journalFile.toPath());
            final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            final DataInputStream dis = new DataInputStream(bais);

            final String codecName = dis.readUTF();
            assertEquals(StandardEventSerializer.CODEC_NAME, codecName);
            
            final int version = dis.readInt();
            assertEquals(1, version);
            
            // compression flag
            assertEquals(true, dis.readBoolean());
            
            // read block start
            final CompressionInputStream decompressedIn = new CompressionInputStream(bais);
            final StandardEventDeserializer deserializer = new StandardEventDeserializer();
            
            final DataInputStream decompressedDis = new DataInputStream(decompressedIn);
            final int eventLength = decompressedDis.readInt();
            assertEquals(131, eventLength);
            final ProvenanceEventRecord event = deserializer.deserialize(decompressedDis, 0);
            assertEquals(1, event.getEventId());
            assertEquals(ProvenanceEventType.CREATE, event.getEventType());
            
            assertEquals(-1, decompressedIn.read());
        } finally {
            journalFile.delete();
        }
    }
    
    @Test
    public void testManyBlocksOneRecordWriteCompressed() throws IOException {
        final File journalFile = new File("target/" + UUID.randomUUID().toString());
        
        final StandardEventSerializer serializer = new StandardEventSerializer();
        try {
            try (final StandardJournalWriter writer = new StandardJournalWriter(1L, journalFile, true, serializer)) {
                for (int i=0; i < 1024; i++) {
                    writer.beginNewBlock();
                    writer.write(Collections.singleton(TestUtil.generateEvent(1L)), 1L);
                    writer.finishBlock();
                }
            }
        
            final byte[] data = Files.readAllBytes(journalFile.toPath());
            final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            final DataInputStream dis = new DataInputStream(bais);

            final String codecName = dis.readUTF();
            assertEquals(StandardEventSerializer.CODEC_NAME, codecName);
            
            final int version = dis.readInt();
            assertEquals(1, version);
            
            // compression flag
            assertEquals(true, dis.readBoolean());
            
            // read block start
            for (int i=0; i < 1024; i++) {
                final CompressionInputStream decompressedIn = new CompressionInputStream(bais);
                final StandardEventDeserializer deserializer = new StandardEventDeserializer();
                
                final DataInputStream decompressedDis = new DataInputStream(decompressedIn);
                final int eventLength = decompressedDis.readInt();
                assertEquals(131, eventLength);
                final ProvenanceEventRecord event = deserializer.deserialize(decompressedDis, 0);
                assertEquals(1, event.getEventId());
                assertEquals(ProvenanceEventType.CREATE, event.getEventType());
                
                if ( i == 1023 ) {
                    assertEquals(-1, decompressedIn.read());
                }
            }
        } finally {
            journalFile.delete();
        }
    }
    
    
}
