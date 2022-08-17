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
package org.apache.nifi.registry.provider.flow;

import org.apache.nifi.registry.db.DatabaseBaseTest;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class TestDatabaseFlowPersistenceProvider extends DatabaseBaseTest {

    @Autowired
    private DataSource dataSource;

    private FlowPersistenceProvider persistenceProvider;

    @Before
    public void setup() {
        persistenceProvider = new DatabaseFlowPersistenceProvider();
        ((DatabaseFlowPersistenceProvider)persistenceProvider).setDataSource(dataSource);
    }

    @Test
    public void testAll() {
        // Save two versions of a flow...
        final FlowSnapshotContext context1 = getFlowSnapshotContext("b1", "f1", 1);
        final byte[] content1 = "f1v1".getBytes(StandardCharsets.UTF_8);
        persistenceProvider.saveFlowContent(context1, content1);

        final FlowSnapshotContext context2 = getFlowSnapshotContext("b1", "f1", 2);
        final byte[] content2 = "f1v2".getBytes(StandardCharsets.UTF_8);
        persistenceProvider.saveFlowContent(context2, content2);

        // Verify we can retrieve both versions and that the content is correct
        final byte[] retrievedContent1 = persistenceProvider.getFlowContent(context1.getBucketId(), context1.getFlowId(), context1.getVersion());
        assertNotNull(retrievedContent1);
        assertEquals("f1v1", new String(retrievedContent1, StandardCharsets.UTF_8));

        final byte[] retrievedContent2 = persistenceProvider.getFlowContent(context2.getBucketId(), context2.getFlowId(), context2.getVersion());
        assertNotNull(retrievedContent2);
        assertEquals("f1v2", new String(retrievedContent2, StandardCharsets.UTF_8));

        // Delete a specific version and verify we can longer retrieve it
        persistenceProvider.deleteFlowContent(context1.getBucketId(), context1.getFlowId(), context1.getVersion());

        final byte[] deletedContent1 = persistenceProvider.getFlowContent(context1.getBucketId(), context1.getFlowId(), context1.getVersion());
        assertNull(deletedContent1);

        // Delete all content for a flow
        persistenceProvider.deleteAllFlowContent(context1.getBucketId(), context1.getFlowId());

        final byte[] deletedContent2 = persistenceProvider.getFlowContent(context2.getBucketId(), context2.getFlowId(), context2.getVersion());
        assertNull(deletedContent2);
    }

    private FlowSnapshotContext getFlowSnapshotContext(final String bucketId, final String flowId, final int version) {
        final FlowSnapshotContext context = Mockito.mock(FlowSnapshotContext.class);
        when(context.getBucketId()).thenReturn(bucketId);
        when(context.getFlowId()).thenReturn(flowId);
        when(context.getVersion()).thenReturn(version);
        return context;
    }

}
