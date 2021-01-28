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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.UUID;

import org.junit.Test;

public class TestCompoundUpdateMonitor {

    @Test
    public void test() throws IOException {
        final UpdateMonitor lastModified = new LastModifiedMonitor();
        final DigestUpdateMonitor updateMonitor = new DigestUpdateMonitor();
        final CompoundUpdateMonitor compound = new CompoundUpdateMonitor(lastModified, updateMonitor);

        final File file = new File("target/" + UUID.randomUUID().toString());
        if (file.exists()) {
            assertTrue(file.delete());
        }
        assertTrue(file.createNewFile());

        final Path path = file.toPath();

        final Object curState = compound.getCurrentState(path);
        final Object state2 = compound.getCurrentState(path);

        assertEquals(curState, state2);
        file.setLastModified(System.currentTimeMillis() + 1000L);
        final Object state3 = compound.getCurrentState(path);
        assertEquals(state2, state3);

        final Object state4 = compound.getCurrentState(path);
        assertEquals(state3, state4);

        final long lastModifiedDate = file.lastModified();
        try (final OutputStream out = new FileOutputStream(file)) {
            out.write("Hello".getBytes("UTF-8"));
        }

        file.setLastModified(lastModifiedDate);

        final Object state5 = compound.getCurrentState(path);
        assertNotSame(state4, state5);
    }

}
