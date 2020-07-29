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
package org.apache.nifi.processors.kafka.pubsub;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * Collection of implementation of common Kafka {@link Partitioner}s.
 */
final public class Partitioners {

    private Partitioners() {
    }

    /**
     * {@link Partitioner} that implements 'round-robin' mechanism which evenly
     * distributes load between all available partitions.
     */
    public static class RoundRobinPartitioner implements Partitioner {

        private volatile int index;

        @Override
        public void configure(Map<String, ?> configs) {
            // noop
        }

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return this.next(cluster.availablePartitionsForTopic(topic).size());
        }

        @Override
        public void close() {
            // noop
        }

        private synchronized int next(int numberOfPartitions) {
            if (this.index >= numberOfPartitions) {
                this.index = 0;
            }
            return index++;
        }
    }
}
