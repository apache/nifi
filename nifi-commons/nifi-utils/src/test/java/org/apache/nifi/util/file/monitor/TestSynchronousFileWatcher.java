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
package org.apache.nifi.util.file.monitor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.junit.Test;

public class TestSynchronousFileWatcher {

    @Test
    public void testIt() throws UnsupportedEncodingException, IOException, InterruptedException {
        final Path path = Paths.get("target/1.txt");
        Files.copy(new ByteArrayInputStream("Hello, World!".getBytes("UTF-8")), path, StandardCopyOption.REPLACE_EXISTING);
        final UpdateMonitor monitor = new DigestUpdateMonitor();

        final SynchronousFileWatcher watcher = new SynchronousFileWatcher(path, monitor, 10L);
        assertFalse(watcher.checkAndReset());
        Thread.sleep(30L);
        assertFalse(watcher.checkAndReset());

        final FileOutputStream fos = new FileOutputStream(path.toFile());
        try {
            fos.write("Good-bye, World!".getBytes("UTF-8"));
            fos.getFD().sync();
        } finally {
            fos.close();
        }

        assertTrue(watcher.checkAndReset());
        assertFalse(watcher.checkAndReset());

        Thread.sleep(30L);
        assertFalse(watcher.checkAndReset());
    }
}
