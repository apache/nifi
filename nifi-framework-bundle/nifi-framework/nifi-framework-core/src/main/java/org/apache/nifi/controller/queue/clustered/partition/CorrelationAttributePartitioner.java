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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CorrelationAttributePartitioner implements FlowFilePartitioner {
    private static final int INDEX_OFFSET = 1;

    // Multiplier from com.google.common.hash.Hashing.LinearCongruentialGenerator
    private static final long LCG_MULTIPLIER = 2862933555777941757L;

    private static final Logger logger = LoggerFactory.getLogger(CorrelationAttributePartitioner.class);

    private final String partitioningAttribute;

    public CorrelationAttributePartitioner(final String partitioningAttribute) {
        this.partitioningAttribute = partitioningAttribute;
    }

    @Override
    public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions,  final QueuePartition localPartition) {
        final int hash = hash(flowFile);

        final int index = findIndex(hash, partitions.length);

        if (logger.isDebugEnabled()) {
            final List<String> partitionDescriptions = new ArrayList<>(partitions.length);
            for (final QueuePartition partition : partitions) {
                partitionDescriptions.add(partition.getSwapPartitionName());
            }

            logger.debug("Assigning Partition {} to {} based on {}", index, flowFile.getAttribute(CoreAttributes.UUID.key()), partitionDescriptions);
        }

        return partitions[index];
    }

    protected int hash(final FlowFileRecord flowFile) {
        final String partitionAttributeValue = flowFile.getAttribute(partitioningAttribute);
        return (partitionAttributeValue == null) ? 0 : partitionAttributeValue.hashCode();
    }

    @Override
    public boolean isRebalanceOnClusterResize() {
        return true;
    }

    @Override
    public boolean isRebalanceOnFailure() {
        return false;
    }

    private int findIndex(final long hash, final int partitions) {
        // Method implementation based on Google Guava com.google.common.hash.Hashing.consistentHash()
        final LinearCongruentialGenerator generator = new LinearCongruentialGenerator(hash);
        int candidate = 0;

        while (true) {
            final double nextGenerated = generator.nextDouble();
            final int nextCandidate = candidate + INDEX_OFFSET;
            final int next = (int) (nextCandidate / nextGenerated);
            if (next >= 0 && next < partitions) {
                candidate = next;
            } else {
                final int index;
                if (candidate == 0) {
                    index = candidate;
                } else {
                    // Adjust index when handling more than one partition
                    index = candidate - INDEX_OFFSET;
                }
                return index;
            }
        }
    }

    private static final class LinearCongruentialGenerator {
        private long state;

        private LinearCongruentialGenerator(final long seed) {
            this.state = seed;
        }

        private double nextDouble() {
            state = LCG_MULTIPLIER * state + INDEX_OFFSET;
            return ((double) ((int) (state >>> 33) + 1)) / 0x1.0p31;
        }
    }
}
