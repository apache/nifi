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

import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

public interface KafkaProducerService extends Closeable {

    /**
     * Send the record(s) associated with a single FlowFile.
     *
     * @param records        the NiFi representation of the Kafka records to be published
     * @param publishContext the NiFi context associated with the publish attempt
     */
    void send(Iterator<KafkaRecord> records, PublishContext publishContext);

    /**
     * Signal the Kafka `Producer` to carry out publishing of the message(s).  This has significance in the case of
     * transactional publish activity.
     */
    RecordSummary complete();

    /**
     * Signal the Kafka `Producer` to close the producer connection.  This allows for graceful handling of
     * `ControllerService` misconfiguration issues (NIFI-12194).
     */
    void close();

    /**
     * @return true if the producer is closed, false otherwise
     */
    boolean isClosed();

    /**
     * Fetch metadata associated with the Kafka partitions associated with the topic.
     */
    List<PartitionState> getPartitionStates(String topic);
}
