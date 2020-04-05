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
package org.apache.nifi.hdfs.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class ContainerTest {

    @Test
    public void initialStateTest() throws IOException {
        Container container = new Container("test", new Path("src/test/resources"), new Configuration(), 1024, 1024 * 1024, true);

        // just making sure file system creation works
        Path path = container.getPath();
        FileSystem fs = container.getFileSystem();
        assertTrue(fs.exists(path));

        // make sure the container is the in expected initial state
        assertFalse(container.isFull());
        assertFalse(container.isFailedRecently());
        assertTrue(container.isActive());
        assertEquals(0, container.getLastFailure());
    }

    @Test
    public void fullDiskTest() throws IOException {
        Container container = new Container("test", new Path("src/test/resources"), new Configuration(), 1024, 1024 * 1024, true);

        container.setFull(true);

        assertTrue(container.isFull());
        assertFalse(container.isActive());
        assertFalse(container.isFailedRecently());
        assertEquals(0, container.getLastFailure());

        // should have no effect since it's already full
        container.setFull(true);

        assertTrue(container.isFull());
        assertFalse(container.isActive());
        assertFalse(container.isFailedRecently());
        assertEquals(0, container.getLastFailure());

        container.setFull(false);

        // should now be active
        assertFalse(container.isFull());
        assertTrue(container.isActive());
        assertFalse(container.isFailedRecently());
        assertEquals(0, container.getLastFailure());
    }

    @Test
    public void failureTest() throws InterruptedException, IOException {
        Container container = new Container("test", new Path("src/test/resources"), new Configuration(), 1024, 1024 * 1024, true);

        long start = System.currentTimeMillis();

        container.failureOcurred();

        assertFalse(container.isFull());
        assertFalse(container.isActive());
        assertTrue(container.isFailedRecently());

        long lastFailure = container.getLastFailure();
        assertTrue(lastFailure >= start);

        Thread.sleep(2);

        long second = System.currentTimeMillis();

        container.failureOcurred();

        long newLastFailure = container.getLastFailure();
        assertFalse(container.isFull());
        assertFalse(container.isActive());
        assertTrue(container.isFailedRecently());
        assertTrue(lastFailure < second);
        assertNotEquals(lastFailure, newLastFailure);
        assertTrue(newLastFailure >= second);

        // simulate incorrect failure reset condition,
        // failure status shouldn't be cleared
        assertFalse(container.clearFailure(start));
        assertFalse(container.isFull());
        assertFalse(container.isActive());
        assertTrue(container.isFailedRecently());
        assertEquals(newLastFailure, container.getLastFailure());

        // now actually clear the failure
        assertTrue(container.clearFailure(newLastFailure));
        assertFalse(container.isFull());
        assertTrue(container.isActive());
        assertFalse(container.isFailedRecently());
        assertEquals(newLastFailure, container.getLastFailure());
    }

    @Test
    public void failuresDisabledTest() throws IOException {
        Container container = new Container("test", new Path("src/test/resources"), new Configuration(), 1024, 1024 * 1024, false);

        container.failureOcurred();

        assertFalse(container.isFull());
        assertTrue(container.isActive());
        assertFalse(container.isFailedRecently());
    }

    @Test
    public void fullThenFailureTest() throws IOException {
        Container container = new Container("test", new Path("src/test/resources"), new Configuration(), 1024, 1024 * 1024, true);

        container.setFull(true);
        container.failureOcurred();

        assertTrue(container.isFull());
        assertFalse(container.isActive());
        assertTrue(container.isFailedRecently());

        // now clear the failure, the container should still be inactive because it's full
        assertTrue(container.clearFailure(container.getLastFailure()));
        assertTrue(container.isFull());
        assertFalse(container.isActive());
        assertFalse(container.isFailedRecently());

        // now clear full and make sure it's active again
        container.setFull(false);
        assertFalse(container.isFull());
        assertTrue(container.isActive());
        assertFalse(container.isFailedRecently());
    }

    @Test
    public void failureThenFullTest() throws IOException {
        Container container = new Container("test", new Path("src/test/resources"), new Configuration(), 1024, 1024 * 1024, true);

        container.setFull(true);
        container.failureOcurred();

        assertTrue(container.isFull());
        assertFalse(container.isActive());
        assertTrue(container.isFailedRecently());

        // container should still be inactive because it failed recently
        container.setFull(false);
        assertFalse(container.isFull());
        assertFalse(container.isActive());
        assertTrue(container.isFailedRecently());

        // now clear the failure, the container should still be active again
        assertTrue(container.clearFailure(container.getLastFailure()));
        assertFalse(container.isFull());
        assertTrue(container.isActive());
        assertFalse(container.isFailedRecently());
    }

}
