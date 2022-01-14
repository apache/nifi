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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.nifi.pmem.PmemMappedFileTestUtils.NON_PMEM_FS_DIR;
import static org.apache.nifi.pmem.PmemMappedFileTestUtils.READ_WRITE_MODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class TestPmemMappedFileNonPmemFs {
    @Rule(order = 0)
    public final AssumePmemFsDirExists assumption = new AssumePmemFsDirExists();

    @Rule(order = 1)
    public final TemporaryFolder tempDir = TemporaryFolder.builder()
            .parentFolder(NON_PMEM_FS_DIR.toFile()).assureDeletion().build();

    private static final int OFFSET = 5;
    private static final int REMAINING = 10;
    private static final int WRITE_LENGTH = 42;

    private Path path = null;
    private PmemMappedFile pmem = null;

    @Before
    public void setUp() throws Exception {
        path = tempDir.newFile().toPath();
        Files.delete(path);

        pmem = PmemMappedFile.create(path.toString(), PmemMappedFileTestUtils.SMALL_LENGTH, READ_WRITE_MODE);
        assertNotNull(pmem);
        assertEquals(path.toString(), pmem.path());
        assertEquals(PmemMappedFileTestUtils.SMALL_LENGTH, pmem.length());
        assertFalse(pmem.isPmem()); // because the path is not on PMEM
        // do not care pmem.isHugeAligned()
    }

    @After
    public void tearDown() throws Exception {
        if (pmem != null) {
            pmem.close();
        }
    }

    @Test
    public void testMsync() throws IOException {
        assertSame(pmem, pmem.putNoFlush(OFFSET, new byte[WRITE_LENGTH], 0, WRITE_LENGTH));
        assertSame(pmem, pmem.msync(OFFSET, WRITE_LENGTH));
    }
}
