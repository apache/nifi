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
package org.apache.nifi.controller.queue.clustered.partition


import org.apache.nifi.controller.repository.FlowFileRecord
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class NonLocalPartitionPartitionerSpec extends Specification {

    def "getPartition chooses local partition with 1 partition and throws IllegalStateException"() {
        given: "a local partitioner using a local partition"
        def partitioner = new NonLocalPartitionPartitioner()
        def localPartition = Mock QueuePartition
        def partitions = [localPartition] as QueuePartition[]
        def flowFileRecord = Mock FlowFileRecord

        when: "a partition is requested from the partitioner"
        partitioner.getPartition flowFileRecord, partitions, localPartition

        then: "an IllegalStateExceptions thrown"
        thrown(IllegalStateException)
    }

    @Unroll
    def "getPartition chooses non-local partition with #maxPartitions partitions, #threads threads, #iterations iterations"() {
        given: "a local partitioner"
        def partitioner = new NonLocalPartitionPartitioner()
        def partitions = new QueuePartition[maxPartitions]

        and: "a local partition"
        def localPartition = Mock QueuePartition
        partitions[0] = localPartition

        and: "one or more multiple partitions"
        for (int id = 1; id < maxPartitions; ++id) {
            def partition = Mock QueuePartition
            partitions[id] = partition
        }

        and: "an array to hold the resulting chosen partitions and an executor service with one or more threads"
        def flowFileRecord = Mock FlowFileRecord
        def chosenPartitions = [] as ConcurrentLinkedQueue
        def executorService = Executors.newFixedThreadPool threads

        when: "a partition is requested from the partitioner for a given flowfile record and the existing partitions"
        iterations.times {
            executorService.submit {
                chosenPartitions.add partitioner.getPartition(flowFileRecord, partitions, localPartition)
            }
        }
        executorService.shutdown()
        try {
            while (!executorService.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                Thread.sleep(10)
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow()
            Thread.currentThread().interrupt()
        }

        then: "no exceptions are thrown"
        noExceptionThrown()

        and: "there is a chosen partition for each iteration"
        chosenPartitions.size() == iterations

        and: "each chosen partition is a remote partition and is one of the existing partitions"
        def validChosenPartitions = chosenPartitions.findAll { it != localPartition && partitions.contains(it) }

        and: "there is a valid chosen partition for each iteration"
        validChosenPartitions.size() == iterations

        and: "there are no other mock interactions"
        0 * _

        where:
        maxPartitions | threads | iterations
        2             | 1       | 1
        2             | 1       | 10
        2             | 1       | 100
        2             | 10      | 1000
        5             | 1       | 1
        5             | 1       | 10
        5             | 1       | 100
        5             | 10      | 1000
    }
}
