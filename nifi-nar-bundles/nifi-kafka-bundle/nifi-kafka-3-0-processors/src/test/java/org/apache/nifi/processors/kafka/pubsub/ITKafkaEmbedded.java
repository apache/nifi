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

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test simple interaction with dynamic instance of Kafka 3.0 service.  Kafka service is started and stopped in
 * the context of the unit test.  To avoid port conflicts with any running Kafka instance on localhost, non-default
 * ports are used.
 *
 * This test might be useful in distinguishing NiFi client library usage issues from issues with the kafka client
 * libraries themselves.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ITKafkaEmbedded {
    private static final Logger logger = LoggerFactory.getLogger(ITKafkaEmbedded.class);

    private static TestingServer zookeeperServer;
    private static CuratorFramework curatorFramework;
    private static KafkaServer kafkaServer;

    /**
     * The Kafka endpoint for the test class.
     */
    private static final int ZOOKEEPER_PORT = 2182;  // use non-default (2181) to avoid conflict with running instance
    private static final int BOOTSTRAP_PORT = 9192;  // use non-default (9092) to avoid conflict with running instance
    private static final String BOOTSTRAP_SERVER = String.format("localhost:%d", BOOTSTRAP_PORT);

    /**
     * Ensure fresh data for each test run.
     */
    private static final long TIMESTAMP = System.currentTimeMillis();

    /**
     * The name of the test kafka topic to be created.
     */
    private static final String TOPIC_NAME = "nifi-events-" + TIMESTAMP;

    /**
     * Number of test events to send.
     */
    private static final int EVENT_COUNT = 3;

    /**
     * Target folder for embedded Kafka server state.
     */
    private static File kafkaFolderIT;

    /**
     * Test framework should be configured to use Kafka 3 release.
     */
    @Test
    public void testKafkaVersion() {
        final Class<KafkaServer> c = KafkaServer.class;
        final URL url = c.getProtectionDomain().getCodeSource().getLocation();
        assertEquals("file", url.getProtocol());
        final String filename = new File(url.getFile()).getName();
        assertTrue(filename.contains("3.0.0"));
    }

    /**
     * Validate expectations of default Kafka configuration values.
     */
    @Test
    public void testKafkaConfig() {
        final Map<String, String> properties = new TreeMap<>();
        properties.put(KafkaConfig.ZkConnectProp(), "");
        final KafkaConfig config = new KafkaConfig(properties);
        final String kafkaPathDefault = config.getString(KafkaConfig.LogDirProp());
        assertNotNull(kafkaPathDefault);
        final File folderKafkaDefault = new File(kafkaPathDefault);
        assertTrue(folderKafkaDefault.getParentFile().exists());
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        logger.trace("ENTERING beforeClass()");
        zookeeperServer = new TestingServer(ZOOKEEPER_PORT);
        curatorFramework = CuratorFrameworkFactory.newClient(
                zookeeperServer.getConnectString(), new RetryOneTime(2000));
        curatorFramework.start();

        final Map<String, String> properties = new TreeMap<>();
        properties.put(KafkaConfig.ZkConnectProp(), zookeeperServer.getConnectString());
        properties.put(KafkaConfig.ZkConnectionTimeoutMsProp(), "5000");

        // redirect Kafka target folder to a custom location for this test
        final KafkaConfig configGetDefault = new KafkaConfig(properties);
        final String kafkaPathDefault = configGetDefault.getString(KafkaConfig.LogDirProp());
        assertNotNull(kafkaPathDefault);
        final File folderKafkaDefault = new File(kafkaPathDefault);
        assertTrue(folderKafkaDefault.getParentFile().exists());
        kafkaFolderIT = new File(folderKafkaDefault.getParentFile(),
                String.format("%s-%d", folderKafkaDefault.getName(), TIMESTAMP));
        logger.debug(kafkaFolderIT.getPath());

        properties.put(KafkaConfig.ListenersProp(), String.format("PLAINTEXT://:%d", BOOTSTRAP_PORT));
        properties.put(KafkaConfig.LogDirProp(), kafkaFolderIT.getPath());
        properties.put(KafkaConfig.LogDirsProp(), kafkaFolderIT.getPath());
        properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        properties.put(KafkaConfig.GroupInitialRebalanceDelayMsProp(), "0");

        final KafkaConfig config = new KafkaConfig(properties);
        kafkaServer = new KafkaServer(config, Time.SYSTEM, Option.empty(), false);
        kafkaServer.startup();
        logger.trace("EXITING beforeClass()");
    }

    @Test
    public void testKafka3_StepA_CreateTopic() throws ExecutionException, InterruptedException {
        logger.trace("ENTERING testKafka3_StepA_CreateTopic(): {}", TOPIC_NAME);
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        final AdminClient adminClient = AdminClient.create(properties);
        final NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
        final CreateTopicsResult topics = adminClient.createTopics(Collections.singleton(newTopic));
        final KafkaFuture<Void> topicFuture = topics.values().get(TOPIC_NAME);
        topicFuture.get();  // throw on commit failure
        logger.trace("EXITING testKafka3_StepA_CreateTopic(): {}", TOPIC_NAME);
    }

    @Test
    public void testKafka3_StepB_ProducerWriteMessage() throws ExecutionException, InterruptedException {
        logger.trace("ENTERING testKafka3_Step1_ProducerWriteMessage()");
        final long timestamp = System.currentTimeMillis();
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; (i < EVENT_COUNT); ++i) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_NAME, "mykey", toDateString(timestamp));
            logger.debug("RECORD TO SEND: {}", record.value());
            final Future<RecordMetadata> future = producer.send(record);
            final RecordMetadata metadata = future.get();
            logger.debug("RECORD SENT: OFFSET={}  TIMESTAMP={}",
                    metadata.hasOffset() ? metadata.offset() : "",
                    metadata.hasTimestamp() ? toDateString(metadata.timestamp()) : "");
            assertTrue(metadata.hasOffset());
            assertTrue(metadata.hasTimestamp());
            assertEquals(i, metadata.offset());
            assertTrue(timestamp < metadata.timestamp());
            assertTrue(System.currentTimeMillis() > metadata.timestamp());
        }
        producer.close();
        logger.trace("EXITING testKafka3_StepB_ProducerWriteMessage()");
    }

    @Test
    public void testKafka3_StepC1_ConsumerReadMessageSubscribe() {
        logger.trace("ENTERING testKafka3_StepC1_ConsumerReadMessageSubscribe()");
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        final Set<String> subscription = consumer.subscription();
        assertEquals(1, subscription.size());
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC_NAME);
        assertEquals(1, partitionInfos.size());
        final Collection<ConsumerRecord<String, String>> records = queryRecords(consumer, 10);
        assertEquals(EVENT_COUNT, records.size());
        consumer.unsubscribe();
        assertEquals(0, consumer.assignment().size());
        logger.trace("EXITING testKafka3_StepC1_ConsumerReadMessageSubscribe()");
    }

    @Test
    public void testKafka3_StepC2_ConsumerReadMessageAssign() {
        logger.trace("ENTERING testKafka3_StepC2_ConsumerReadMessageAssign()");
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        final TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Collections.singletonList(partition0));
        final Set<TopicPartition> assignments = consumer.assignment();
        assertEquals(1, assignments.size());
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC_NAME);
        assertEquals(1, partitionInfos.size());
        final Collection<ConsumerRecord<String, String>> records = queryRecords(consumer, 10);
        assertEquals(EVENT_COUNT, records.size());
        consumer.unsubscribe();
        assertEquals(0, consumer.assignment().size());
        logger.trace("EXITING testKafka3_StepC2_ConsumerReadMessageAssign()");
    }

    private Collection<ConsumerRecord<String, String>> queryRecords(
            final KafkaConsumer<String, String> consumer, final int iterations) {
        final Collection<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; (i < iterations); ++i) {
            final ConsumerRecords<String, String> recordsIt = consumer.poll(Duration.ofMillis(100));
            for (final ConsumerRecord<String, String> record : recordsIt) {
                records.add(record);
                logger.debug("RECORD @ {}/{} {}={}", record.partition(), record.offset(), record.key(), record.value());
            }
        }
        return records;
    }

    @AfterAll
    public static void afterClass() throws Exception {
        logger.trace("ENTERING afterClass()");
        kafkaServer.shutdown();
        curatorFramework.close();
        zookeeperServer.stop();
        discardTestState(kafkaFolderIT);
        logger.trace("EXITING afterClass()");
    }

    private String toDateString(final long timestampMillis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis),
                ZoneId.of(ZoneOffset.UTC.getId())).format(DateTimeFormatter.ISO_INSTANT);
    }

    private static void discardTestState(final File folder) {
        logger.debug(folder.getPath());
        final Queue<File> filesToDiscard = new LinkedList<>(Collections.singleton(folder));
        while (!filesToDiscard.isEmpty()) {
            final File file = filesToDiscard.poll();
            logger.trace("POLL {}", file.getPath());
            if (file.isDirectory()) {
                final File[] filesChild = file.listFiles();
                if ((filesChild == null) || (filesChild.length == 0)) {
                    final boolean delete = file.delete();
                    logger.trace("DELETE {} {}", file.getPath(), delete);
                } else {
                    filesToDiscard.addAll(Arrays.asList(filesChild));
                    filesToDiscard.add(file);
                }
            } else if (file.isFile()) {
                final boolean delete = file.delete();
                logger.trace("DELETE {} {}", file.getPath(), delete);
            }
        }
    }
}
