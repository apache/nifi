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

package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CorrelationAttributePartitionerTest {
    private static final String PARTITIONING_ATTRIBUTE = "group";

    private static final String FIRST_ATTRIBUTE = "1";

    private static final String SECOND_ATTRIBUTE = "2";

    @Mock
    private FlowFileRecord flowFileRecord;

    @Mock
    private QueuePartition localPartition;

    @Mock
    private QueuePartition firstPartition;

    @Mock
    private QueuePartition secondPartition;

    @Mock
    private QueuePartition thirdPartition;

    private CorrelationAttributePartitioner partitioner;

    @BeforeEach
    void setPartitioner() {
        partitioner = new CorrelationAttributePartitioner(PARTITIONING_ATTRIBUTE);
    }

    @Test
    void testRebalanceOnClusterResize() {
        assertTrue(partitioner.isRebalanceOnClusterResize());
    }

    @Test
    void testRebalanceOnFailure() {
        assertFalse(partitioner.isRebalanceOnFailure());
    }

    @Test
    void testGetPartitionOnePartitionNullAttribute() {
        final QueuePartition[] partitions = new QueuePartition[]{firstPartition};

        final QueuePartition partition = partitioner.getPartition(flowFileRecord, partitions, localPartition);

        assertEquals(firstPartition, partition);
    }

    @Test
    void testGetPartitionTwoPartitionsNullAttribute() {
        final QueuePartition[] partitions = new QueuePartition[]{firstPartition, secondPartition};

        final QueuePartition partition = partitioner.getPartition(flowFileRecord, partitions, localPartition);

        assertEquals(firstPartition, partition);
    }

    @Test
    void testGetPartitionThreePartitionsAttributeDefined() {
        final QueuePartition[] partitions = new QueuePartition[]{firstPartition, secondPartition, thirdPartition};

        // Set First Attribute for partitioning
        when(flowFileRecord.getAttribute(eq(PARTITIONING_ATTRIBUTE))).thenReturn(FIRST_ATTRIBUTE);

        final QueuePartition firstSelected = partitioner.getPartition(flowFileRecord, partitions, localPartition);
        assertEquals(firstPartition, firstSelected);

        // Set Second Attribute for partitioning
        when(flowFileRecord.getAttribute(eq(PARTITIONING_ATTRIBUTE))).thenReturn(SECOND_ATTRIBUTE);

        final QueuePartition secondSelected = partitioner.getPartition(flowFileRecord, partitions, localPartition);
        assertEquals(secondPartition, secondSelected);

        final QueuePartition thirdSelected = partitioner.getPartition(flowFileRecord, partitions, localPartition);
        assertEquals(secondPartition, thirdSelected);

        // Reset to First Attribute for partitioning
        when(flowFileRecord.getAttribute(eq(PARTITIONING_ATTRIBUTE))).thenReturn(FIRST_ATTRIBUTE);

        final QueuePartition fourthSelected = partitioner.getPartition(flowFileRecord, partitions, localPartition);
        assertEquals(firstPartition, fourthSelected);
    }
}
