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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.nifi.pmem.PmemMappedFileTestUtils.READ_WRITE_MODE;
import static org.apache.nifi.pmem.PmemMappedFileTestUtils.PMEM_FS_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestPmemMappedFilePutBounds {
    @Rule(order = 0)
    public final AssumePmemFsDirExists assumption = new AssumePmemFsDirExists();

    @Rule(order = 1)
    public final TemporaryFolder tempDir = TemporaryFolder.builder()
            .parentFolder(PMEM_FS_DIR.toFile()).assureDeletion().build();

    private PmemMappedFile pmem = null;

    @Before
    public void setUp() throws Exception {
        final Path path = tempDir.newFile().toPath();
        Files.delete(path);

        pmem = PmemMappedFile.create(path.toString(), PmemMappedFileTestUtils.SMALL_LENGTH, READ_WRITE_MODE);
        assertNotNull(pmem);
        assertEquals(path.toString(), pmem.path());
        assertEquals(PmemMappedFileTestUtils.SMALL_LENGTH, pmem.length());
        assertTrue(pmem.isPmem());
        assertTrue(pmem.isHugeAligned());
    }

    @After
    public void tearDown() throws Exception {
        if (pmem != null) {
            pmem.close();
        }
    }

    // -- putNoDrain -- //

    @Test
    public void testPutNoDrainBoundsOffsetOverflow() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoDrain(
                pmem.length() + 1L, new byte[1], 0, 0));
    }

    @Test
    public void testPutNoDrainBoundsOffsetNegative() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoDrain(
                -1L, new byte[1], 0, 0));
    }

    @Test
    public void testPutNoDrainBoundsIndexOverflow() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoDrain(
                0L, new byte[1], 2, 0));
    }

    @Test
    public void testPutNoDrainBoundsIndexNegative() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoDrain(
                0L, new byte[1], -1, 0));
    }

    @Test
    public void testPutNoDrainBoundsLengthOverflow() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoDrain(
                0L, new byte[1], 0, 2));
    }

    @Test
    public void testPutNoDrainBoundsLengthNegative() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoDrain(
                0L, new byte[1], 0, -1));
    }

    // -- putNoFlush -- //

    @Test
    public void testPutNoFlushBoundsOffsetOverflow() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoFlush(
                pmem.length() + 1L, new byte[1], 0, 0));
    }

    @Test
    public void testPutNoFlushBoundsOffsetNegative() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoFlush(
                -1L, new byte[1], 0, 0));
    }

    @Test
    public void testPutNoFlushBoundsIndexOverflow() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoFlush(
                0L, new byte[1], 2, 0));
    }

    @Test
    public void testPutNoFlushBoundsIndexNegative() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoFlush(
                0L, new byte[1], -1, 0));
    }

    @Test
    public void testPutNoFlushBoundsLengthOverflow() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoFlush(
                0L, new byte[1], 0, 2));
    }

    @Test
    public void testPutNoFlushBoundsLengthNegative() {
        assertThrows(IndexOutOfBoundsException.class, () -> pmem.putNoFlush(
                0L, new byte[1], 0, -1));
    }
}
