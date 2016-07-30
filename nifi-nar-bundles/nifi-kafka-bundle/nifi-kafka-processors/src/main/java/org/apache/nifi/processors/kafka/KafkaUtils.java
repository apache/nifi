package org.apache.nifi.processors.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import kafka.client.ClientUtils;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.network.BlockingChannel;
import kafka.utils.ZkUtils;
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
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import static scala.collection.JavaConversions.asJavaList;
import static scala.collection.JavaConversions.asScalaBuffer;

/**
 * Utility class to support interaction with Kafka internals.
 *
 */
class KafkaUtils {

    private static final int ZK_CONNECTION_TIMEOUT_MS = 30_000;
    private static final int SOCKET_TIMEOUT_MS = 30_000;
    private static final int RETRY_BACK_OFF_MS = 30_000;
    private static final String DEFAULT_CLIENT_ID = "";

    private static class StingSerializer implements ZkSerializer {
        @Override
        public byte[] serialize(Object o) throws ZkMarshallingError {
            return ZKStringSerializer.serialize(o);
        }

        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            return ZKStringSerializer.deserialize(bytes);
        }
    }

    /**
     * Will retrieve the amount of partitions for a given Kafka topic.
     */
    static int retrievePartitionCountForTopic(String zookeeperConnectionString, String topicName) {
        ZkClient zkClient = null;

        try {
            zkClient = new ZkClient(zookeeperConnectionString, ZK_CONNECTION_TIMEOUT_MS);
            zkClient.setZkSerializer(new StingSerializer());
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

    static Map<String, String> retrievePartitionOffsets(final String zookeeperConnectionString, final String topicName,
                                                        final String groupName) throws IOException {
        return executeConsumerGroupCommand(zookeeperConnectionString, topicName, groupName, "retrievePartitionOffsets", (channel, topicPartitions) -> {
            // Use version 0 to retrieve offsets from Zookeeper. version 1 will get from Kafka.
            channel.send(new OffsetFetchRequest(groupName, topicPartitions, (short)0, 0, DEFAULT_CLIENT_ID).underlying());
            final OffsetFetchResponse offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
            final Map<String, String> state = offsetFetchResponse.offsets().entrySet().stream()
                    .filter(entry -> entry.getValue().offset() > -1)
                    .collect(Collectors.toMap(entry -> "partition:" + entry.getKey().partition(), entry -> String.valueOf(entry.getValue().offset())));
            return state;
        });
    }

    static void clearPartitionOffsets(final String zookeeperConnectionString, final String topicName, final String groupName) throws IOException {
        executeConsumerGroupCommand(zookeeperConnectionString, topicName, groupName, "clearPartitionOffsets", (channel, topicPartitions) -> {
            final Map<TopicAndPartition, OffsetAndMetadata> offsets = topicPartitions.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndMetadata(-1, OffsetAndMetadata.NoMetadata(), -1L)));

            // Use version 0 to clear offsets from Zookeeper. version 1 will get from Kafka.
            final OffsetCommitRequest request = new OffsetCommitRequest(groupName, offsets, 0, DEFAULT_CLIENT_ID, (short) 0);
            channel.send(request.underlying());
            final OffsetCommitResponse response = OffsetCommitResponse.readFrom(channel.receive().buffer());
            if (response.hasError()) {
                throw new IOException("Failed to clear offsets due to " + response.errors());
            }

            return true;
        });
    }

    static <T> T executeConsumerGroupCommand(final String zookeeperConnectionString, final String topicName, final String groupId,
                                             final String commandName, final ConsumerGroupCommand<T> command) throws IOException {
        ZkClient zkClient = null;
        BlockingChannel channel = null;

        try {
            zkClient = new ZkClient(zookeeperConnectionString, ZK_CONNECTION_TIMEOUT_MS);
            zkClient.setZkSerializer(new StingSerializer());

            final Buffer<String> topics = asScalaBuffer(Collections.singletonList(topicName));
            final scala.collection.mutable.Map<String, Seq<Object>> topicMap = ZkUtils.getPartitionsForTopics(zkClient, topics);

            final List<TopicAndPartition> topicPartitions = asJavaList(topicMap.get(topicName).get()).stream()
                    .map(p -> new TopicAndPartition(topicName, (Integer) p))
                    .collect(Collectors.toList());

            // Use version 0 to retrieve offsets from Zookeeper. version 1 will get from Kafka.
            channel = ClientUtils.channelToOffsetManager(groupId, zkClient, SOCKET_TIMEOUT_MS, RETRY_BACK_OFF_MS);

            return command.execute(channel, topicPartitions);

        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to " + commandName + " for topic: " + topicName + " consumer group:" + groupId, e);
        } finally {
            try {
                if (channel != null) {
                    channel.disconnect();
                }
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }
    }

    private interface ConsumerGroupCommand<T> {
        T execute(final BlockingChannel channel, final List<TopicAndPartition> topicPartitions) throws IOException;
    }


}
