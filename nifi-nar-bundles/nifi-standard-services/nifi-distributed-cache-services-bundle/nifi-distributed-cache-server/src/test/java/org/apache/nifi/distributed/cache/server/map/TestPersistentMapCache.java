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
package org.apache.nifi.distributed.cache.server.map;

import java.io.File;
import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import org.apache.nifi.distributed.cache.server.EvictionPolicy;
import static org.junit.Assert.fail;
import org.junit.Test;

public class TestPersistentMapCache {

    /**
     * Test OverlappingFileLockException is caught when persistent path is duplicated.
     */
    @Test(expected=OverlappingFileLockException.class)
    public void testDuplicatePersistenceDirectory() {
        try {
            File duplicatedFilePath = new File("/tmp/path1");
            final MapCache cache = new SimpleMapCache("simpleCache", 2, EvictionPolicy.FIFO);
            PersistentMapCache pmc1 = new PersistentMapCache("id1", duplicatedFilePath, cache);
            PersistentMapCache pmc2 = new PersistentMapCache("id2", duplicatedFilePath, cache);
        } catch (IOException ex) {
            fail("Unexpected IOException thrown: " + ex.getMessage());
        }
    }
}