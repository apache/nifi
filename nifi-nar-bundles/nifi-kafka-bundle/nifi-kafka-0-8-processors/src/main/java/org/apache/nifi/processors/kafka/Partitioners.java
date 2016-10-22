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
package org.apache.nifi.processors.kafka;

import java.util.Random;

import kafka.producer.Partitioner;

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
        public int partition(Object key, int numberOfPartitions) {
            int partitionIndex = this.next(numberOfPartitions);
            return partitionIndex;
        }

        private synchronized int next(int numberOfPartitions) {
            if (this.index >= numberOfPartitions) {
                this.index = 0;
            }
            return index++;
        }
    }

    /**
     * {@link Partitioner} that implements 'random' mechanism which randomly
     * distributes the load between all available partitions.
     */
    public static class RandomPartitioner implements Partitioner {
        private final Random random;

        public RandomPartitioner() {
            this.random = new Random();
        }

        @Override
        public int partition(Object key, int numberOfPartitions) {
            return this.random.nextInt(numberOfPartitions);
        }
    }

    /**
     * {@link Partitioner} that implements 'key hash' mechanism which
     * distributes the load between all available partitions based on hashing
     * the value of the key.
     */
    public static class HashPartitioner implements Partitioner {

        @Override
        public int partition(Object key, int numberOfPartitions) {
            if (key != null) {
                return (key.hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
            }
            return 0;
        }
    }
}
