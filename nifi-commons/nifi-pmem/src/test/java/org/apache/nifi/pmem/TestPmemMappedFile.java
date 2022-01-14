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
package org.apache.nifi.pmem;

import org.apache.nifi.pmem.PmemMappedFile.PmemOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.nifi.pmem.PmemMappedFile.PmemExtendStrategy.EXTEND_DOUBLY;
import static org.apache.nifi.pmem.PmemMappedFile.PmemExtendStrategy.EXTEND_TO_FIT;
import static org.apache.nifi.pmem.PmemMappedFile.PmemPutStrategy.PUT_NO_DRAIN;
import static org.apache.nifi.pmem.PmemMappedFile.PmemPutStrategy.PUT_NO_FLUSH;
import static org.apache.nifi.pmem.PmemMappedFileTestUtils.PMEM_FS_DIR;
import static org.apache.nifi.pmem.PmemMappedFileTestUtils.READ_WRITE_MODE;
import static org.apache.nifi.pmem.PmemMappedFileTestUtils.SMALL_LENGTH;
import static org.apache.nifi.pmem.PmemMappedFileTestUtils.newRandomByteArray;
import static org.apache.nifi.pmem.PmemMappedFileTestUtils.safeInt;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestPmemMappedFile {
    @Rule(order = 0)
    public final AssumePmemFsDirExists assumption = new AssumePmemFsDirExists();

    @Rule(order = 1)
    public final TemporaryFolder tempDir = TemporaryFolder.builder()
            .parentFolder(PMEM_FS_DIR.toFile()).assureDeletion().build();

    private static final int OFFSET = 5;
    private static final int REMAINING = 10;
    private static final int WRITE_LENGTH = 42;

    private Path path = null;
    private PmemMappedFile pmem = null;

    @Before
    public void setUp() throws Exception {
        path = tempDir.newFile().toPath();
        Files.delete(path);

        pmem = PmemMappedFile.create(path.toString(), SMALL_LENGTH, READ_WRITE_MODE);
        assertNotNull(pmem);
        assertEquals(path.toString(), pmem.path());
        assertEquals(SMALL_LENGTH, pmem.length());
        assertTrue(pmem.isPmem());
        assertTrue(pmem.isHugeAligned());
    }

    @After
    public void tearDown() throws Exception {
        if (pmem != null) {
            pmem.close();
        }
    }

    @Test
    public void testPutNoDrainPartial() {
        final byte[] src = newRandomByteArray(0L, WRITE_LENGTH);
        assertSame(pmem, pmem.putNoDrain(OFFSET, src, 0, WRITE_LENGTH));
        assertSame(pmem, pmem.drain());

        final ByteBuffer b = pmem.sliceAsReadOnlyBuffer(OFFSET, WRITE_LENGTH);
        assertTrue(b.isDirect());
        assertTrue(b.isReadOnly());

        final byte[] dest = new byte[WRITE_LENGTH];
        b.get(dest);
        assertArrayEquals(src, dest);
    }

    @Test
    public void testPutNoDrainFull() {
        final int length = safeInt(pmem.length());
        final ByteBuffer b = pmem.sliceAsReadOnlyBuffer(0L, length);
        assertTrue(b.isDirect());
        assertTrue(b.isReadOnly());

        final byte[] src = newRandomByteArray(0L, length);
        pmem.putNoDrain(0L, src, 0, length).drain();

        final byte[] dest = new byte[length];
        b.get(dest);

        assertArrayEquals(src, dest);
    }

    @Test
    public void testPutNoDrainNothing() {
        final int length = safeInt(pmem.length());
        final ByteBuffer b = pmem.sliceAsReadOnlyBuffer(0L, length);
        assertTrue(b.isDirect());
        assertTrue(b.isReadOnly());

        final byte[] before = new byte[length];
        b.get(before);

        pmem.putNoDrain(pmem.length(), new byte[0], 0, 0).drain();

        final byte[] after = new byte[length];
        b.rewind();
        b.get(after);

        assertArrayEquals(before, after);
    }

    @Test
    public void testPutNoDrainNullSrc() {
        assertThrows(NullPointerException.class, () -> pmem.putNoDrain(
                0L, null, 0, 0));
    }

    @Test
    public void testPutNoFlushPartial() {
        final byte[] src = newRandomByteArray(0L, WRITE_LENGTH);
        assertSame(pmem, pmem.putNoFlush(OFFSET, src, 0, WRITE_LENGTH));
        assertSame(pmem, pmem.flush(OFFSET, WRITE_LENGTH));
        assertSame(pmem, pmem.drain());

        final ByteBuffer b = pmem.sliceAsReadOnlyBuffer(OFFSET, WRITE_LENGTH);
        assertTrue(b.isDirect());
        assertTrue(b.isReadOnly());

        final byte[] dest = new byte[WRITE_LENGTH];
        b.get(dest);
        assertArrayEquals(src, dest);
    }

    @Test
    public void testPutNoFlushFull() {
        final int length = safeInt(pmem.length());
        final ByteBuffer b = pmem.sliceAsReadOnlyBuffer(0L, length);
        assertTrue(b.isDirect());
        assertTrue(b.isReadOnly());

        final byte[] src = newRandomByteArray(0L, length);
        pmem.putNoFlush(0L, src, 0, length).flush(0L, length).drain();

        final byte[] dest = new byte[length];
        b.get(dest);

        assertArrayEquals(src, dest);
    }

    @Test
    public void testPutNoFlushNothing() {
        final int length = safeInt(pmem.length());
        final ByteBuffer b = pmem.sliceAsReadOnlyBuffer(0L, length);
        assertTrue(b.isDirect());
        assertTrue(b.isReadOnly());

        final byte[] before = new byte[length];
        b.get(before);

        pmem.putNoFlush(pmem.length(), new byte[0], 0, 0).flush(0L, 0).drain();

        final byte[] after = new byte[length];
        b.rewind();
        b.get(after);

        assertArrayEquals(before, after);
    }

    @Test
    public void testPutNoFlushNullSrc() {
        assertThrows(NullPointerException.class, () -> pmem.putNoFlush(
                0, null, 0, 0));
    }

    @Test
    public void testFlushBoundsOffsetOverflow() {
        assertThrows(IndexOutOfBoundsException.class,
                () -> pmem.flush(pmem.length() + 1L, 0L));
    }

    @Test
    public void testFlushBoundsOffsetNegative() {
        assertThrows(IndexOutOfBoundsException.class,
                () -> pmem.flush(-1L, 0L));
    }

    @Test
    public void testFlushBoundsLengthOverflow() {
        assertThrows(IndexOutOfBoundsException.class,
                () -> pmem.flush(0L, pmem.length() + 1L));
    }

    @Test
    public void testFlushBoundsLengthNegative() {
        assertThrows(IndexOutOfBoundsException.class,
                () -> pmem.flush(0L, -1L));
    }

    @Test
    public void testOutputBoundsOverflow() {
        assertThrows(IndexOutOfBoundsException.class,
                () -> pmem.uniqueOutputStream(
                        pmem.length() + 1L, EXTEND_DOUBLY, PUT_NO_FLUSH));
    }

    @Test
    public void testOutputBoundsNegative() {
        assertThrows(IndexOutOfBoundsException.class,
                () -> pmem.uniqueOutputStream(
                        -1L, EXTEND_TO_FIT, PUT_NO_DRAIN));
    }

    @Test
    public void testOutputNullExtend() {
        assertThrows(NullPointerException.class,
                () -> pmem.uniqueOutputStream(
                        0L, null, PUT_NO_DRAIN));
    }

    @Test
    public void testOutputNullPut() {
        assertThrows(NullPointerException.class,
                () -> pmem.uniqueOutputStream(
                        0L, EXTEND_TO_FIT, null));
    }

    @Test
    public void testExtendToFit() throws IOException {
        final long expected = SMALL_LENGTH + WRITE_LENGTH - REMAINING;

        try (final PmemOutputStream out = pmem.uniqueOutputStream(
                SMALL_LENGTH - REMAINING, EXTEND_TO_FIT, PUT_NO_DRAIN)) {
            out.write(new byte[WRITE_LENGTH]); // extend
            out.flush();
            out.sync();

            final PmemMappedFile newPmem = out.underlyingPmem();
            assertNotSame(pmem, newPmem);
            assertEquals(path.toString(), newPmem.path());
            assertTrue(newPmem.isPmem());
            assertTrue(newPmem.isHugeAligned());

            assertEquals(expected, newPmem.length());
        }

        assertEquals(expected, Files.size(path));
    }

    @Test
    public void testExtendDoubly() throws IOException {
        final long expected = SMALL_LENGTH * 2;
        final long expectedShrink = SMALL_LENGTH - REMAINING + WRITE_LENGTH;

        try (final PmemOutputStream out = pmem.uniqueOutputStream(
                SMALL_LENGTH - REMAINING, EXTEND_DOUBLY, PUT_NO_FLUSH)) {
            out.write(new byte[WRITE_LENGTH]); // extend
            out.flush();
            out.sync();

            final PmemMappedFile newPmem = out.underlyingPmem();
            assertNotSame(pmem, newPmem);
            assertEquals(path.toString(), newPmem.path());
            assertTrue(newPmem.isPmem());
            assertTrue(newPmem.isHugeAligned());

            assertEquals(expected, newPmem.length());
        }

        /* Shrink to fit */
        assertEquals(expectedShrink, Files.size(path));
    }

    @Test
    public void testExtendQuadruply() throws IOException {
        final long expected = SMALL_LENGTH * 4;
        final long expectedShrink = SMALL_LENGTH * 2 - REMAINING + WRITE_LENGTH;

        try (final PmemOutputStream out = pmem.uniqueOutputStream(
                SMALL_LENGTH - REMAINING, EXTEND_DOUBLY, PUT_NO_FLUSH)) {
            out.write(new byte[SMALL_LENGTH + WRITE_LENGTH]); // extend
            out.flush();
            out.sync();

            final PmemMappedFile newPmem = out.underlyingPmem();
            assertNotSame(pmem, newPmem);
            assertEquals(path.toString(), newPmem.path());
            assertTrue(newPmem.isPmem());
            assertTrue(newPmem.isHugeAligned());

            assertEquals(expected, newPmem.length());
        }

        /* Shrink to fit */
        assertEquals(expectedShrink, Files.size(path));
    }
}
