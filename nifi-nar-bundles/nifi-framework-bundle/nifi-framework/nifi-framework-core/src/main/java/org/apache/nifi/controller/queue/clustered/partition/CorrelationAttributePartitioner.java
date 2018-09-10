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

import com.google.common.hash.Hashing;
import org.apache.nifi.controller.repository.FlowFileRecord;

public class CorrelationAttributePartitioner implements FlowFilePartitioner {
    private final String partitioningAttribute;

    public CorrelationAttributePartitioner(final String partitioningAttribute) {
        this.partitioningAttribute = partitioningAttribute;
    }

    @Override
    public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions,  final QueuePartition localPartition) {
        final int hash = hash(flowFile);

        // The consistentHash method appears to always return a bucket of '1' if there are 2 possible buckets,
        // so in this case we will just use modulo division to avoid this. I suspect this is a bug with the Guava
        // implementation, but it's not clear at this point.
        final int index;
        if (partitions.length < 3) {
            index = hash % partitions.length;
        } else {
            index = Hashing.consistentHash(hash, partitions.length);
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
}
