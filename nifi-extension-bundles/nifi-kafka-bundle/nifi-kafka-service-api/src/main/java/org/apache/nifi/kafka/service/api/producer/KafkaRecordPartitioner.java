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
package org.apache.nifi.kafka.service.api.producer;

import org.apache.nifi.flowfile.FlowFile;

public interface KafkaRecordPartitioner {

    /**
     * Determines a numeric value to be used as the partition for a Kafka Record that belongs to the given FlowFile
     * and for the given topic. The value returned will not be used directly as the partition number but rather will be
     * converted into a partition number. For example, it will initially be converted into an absolute value and modded
     * by the number of partitions for the topic. However, this logic may change in the future.
     *
     * @param topic    the topic
     * @param flowFile the FlowFile
     * @return a value that can be used to assign a partition to the record
     */
    long partition(String topic, FlowFile flowFile);

}
