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
package org.apache.nifi.provenance.journaling.partition;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.IndexManager;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestQueuingPartitionManager {

    @Test(timeout=5000)
    public void testWriteWaitsForDeletion() throws IOException {
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        config.setCompressOnRollover(false);
        config.setMaxStorageCapacity(50L);
        config.setContainers(containers);
        
        final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);

        final AtomicLong indexSize = new AtomicLong(0L);
        final IndexManager indexManager = Mockito.mock(IndexManager.class);
        Mockito.doAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return indexSize.get();
            }
        }).when(indexManager).getSize(Mockito.any(String.class));
        
        final QueuingPartitionManager mgr = new QueuingPartitionManager(indexManager, new AtomicLong(0L), config, exec, exec);
        
        Partition partition = mgr.nextPartition(true, true);
        assertNotNull(partition);
        
        indexSize.set(1024L);
        partition = mgr.nextPartition(true, false);
        assertNull(partition);
        
        indexSize.set(0L);
        partition = mgr.nextPartition(true, true);
        assertNotNull(partition);
    }
    
}
