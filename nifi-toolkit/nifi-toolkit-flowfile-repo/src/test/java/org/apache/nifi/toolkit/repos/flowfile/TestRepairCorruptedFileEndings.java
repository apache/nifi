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

package org.apache.nifi.toolkit.repos.flowfile;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.junit.Test;

public class TestRepairCorruptedFileEndings {
    private final File targetFile = new File("target/1.bin");

    @Before
    @After
    public void cleanup() {
        if (targetFile.exists()) {
            Assert.assertTrue(targetFile.delete());
        }
    }

    @Test
    public void testEndsWithZeroesGreaterThanBufferSize() throws IOException {
        final byte[] data = new byte[4096 + 8];
        for (int i=0; i < 4096; i++) {
            data[i] = 'A';
        }

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(8, zeroCount);
    }

    @Test
    public void testEndsWithZeroesSmallerThanBufferSize() throws IOException {
        final byte[] data = new byte[1024];
        for (int i = 0; i < 1020; i++) {
            data[i] = 'A';
        }

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(4, zeroCount);
    }

    @Test
    public void testEndsWithZeroesEqualToBufferSize() throws IOException {
        final byte[] data = new byte[4096];
        for (int i = 0; i < 4090; i++) {
            data[i] = 'A';
        }

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(6, zeroCount);
    }


    @Test
    public void testAllZeroesGreaterThanBufferSize() throws IOException {
        final byte[] data = new byte[4096 + 8];

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(4096 + 8, zeroCount);
    }

    @Test
    public void testAllZeroesEqualToBufferSize() throws IOException {
        final byte[] data = new byte[4096];

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(4096, zeroCount);
    }

    @Test
    public void testAllZeroesSmallerThanBufferSize() throws IOException {
        final byte[] data = new byte[1024];

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(1024, zeroCount);
    }


    @Test
    public void testSmallerThanBufferSize() throws IOException {
        final byte[] data = new byte[1024];
        for (int i = 0; i < 1020; i++) {
            data[i] = 'A';
        }

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(4, zeroCount);
    }

    @Test
    public void testSmallerThanBufferSizeNoTrailingZeroes() throws IOException {
        final byte[] data = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            data[i] = 'A';
        }

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(0, zeroCount);
    }


    @Test
    public void testLargerThanBufferSizeNoTrailingZeroes() throws IOException {
        final byte[] data = new byte[8192];
        for (int i = 0; i < 8192; i++) {
            data[i] = 'A';
        }

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(0, zeroCount);
    }


    @Test
    public void testEqualToBufferSizeNoTrailingZeroes() throws IOException {
        final byte[] data = new byte[4096];
        for (int i = 0; i < 4096; i++) {
            data[i] = 'A';
        }

        Files.write(targetFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int zeroCount = RepairCorruptedFileEndings.countTrailingZeroes(targetFile);
        assertEquals(0, zeroCount);
    }

}
