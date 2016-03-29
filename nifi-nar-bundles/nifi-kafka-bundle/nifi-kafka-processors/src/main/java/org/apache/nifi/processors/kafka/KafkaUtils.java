package org.apache.nifi.processors.kafka;

import java.util.Collections;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
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
import org.I0Itec.zkclient.serialize.ZkSerializer;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.utils.ZKStringSerializer;
import scala.collection.JavaConversions;

/**
 * Utility class to support interruction with Kafka internals.
 *
 */
class KafkaUtils {


    /**
     * Will retrieve the amount of partitions for a given Kafka topic.
     */
    static int retrievePartitionCountForTopic(String zookeeperConnectionString, String topicName) {
        ZkClient zkClient = null;

        try {
            zkClient = new ZkClient(zookeeperConnectionString);
            zkClient.setZkSerializer(new ZkSerializer() {
                @Override
                public byte[] serialize(Object o) throws ZkMarshallingError {
                    return ZKStringSerializer.serialize(o);
                }

                @Override
                public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                    return ZKStringSerializer.deserialize(bytes);
                }
            });
            scala.collection.Set<TopicMetadata> topicMetadatas = AdminUtils
                    .fetchTopicMetadataFromZk(JavaConversions.asScalaSet(Collections.singleton(topicName)), zkClient);
            if (topicMetadatas != null && topicMetadatas.size() > 0) {
                return JavaConversions.asJavaSet(topicMetadatas).iterator().next().partitionsMetadata().size();
            } else {
                throw new IllegalStateException("Failed to get metadata for topic " + topicName);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to retrieve partitions for topic " + topicName, e);
        } finally {
            try {
                zkClient.close();
            } catch (Exception e2) {
                // ignore
            }
        }
    }

}
