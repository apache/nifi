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

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class PmemMappedFileTestUtils {
    private static final String PROPERTIES_FILE = "pmem.properties";

    static final int SMALL_LENGTH = (1 << 20) * 4; // 4 MiB
    static final long LARGE_LENGTH = (1L << 30) * 2; // 2 GiB
    static final long DEFAULT_LENGTH = (1L << 30) * 3; // 3 GiB
    static final long NO_SPACE_LENGTH = (1L << 40) * 8; // 8 TiB

    static final Set<PosixFilePermission> READ_WRITE_MODE
            = PosixFilePermissions.fromString("rw-------"); // 0600

    static final Path PMEM_FS_DIR;
    static final Path NON_PMEM_FS_DIR;

    static {
        try {
            final InputStream in = PmemMappedFileTestUtils.class
                    .getClassLoader().getResourceAsStream(PROPERTIES_FILE);
            final Properties props = new Properties();
            props.load(in);

            PMEM_FS_DIR = getPathPropertyOrThrow(props, "pmem.fs.dir");
            NON_PMEM_FS_DIR = getPathPropertyOrThrow(props, "non.pmem.fs.dir");
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private static Path getPathPropertyOrThrow(Properties p, String key) {
        final String value = p.getProperty(key);
        if (value == null) {
            throw new NullPointerException();
        }
        return Paths.get(value);
    }

    static void allocateFile(Path path, long length) throws IOException {
        try (final RandomAccessFile file = new RandomAccessFile(path.toString(), "rw")) {
            file.setLength(length);
        }
    }

    static byte[] newRandomByteArray(long seed, int length) {
        final Random prng = new Random(seed);
        final byte[] array = new byte[length];
        prng.nextBytes(array);
        return array;
    }

    static int safeInt(long j) {
        if (j < Integer.MIN_VALUE || Integer.MAX_VALUE < j) {
            throw new IllegalArgumentException();
        }

        return (int) j;
    }
}
