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

package org.apache.nifi.connectors.kafkas3;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorConfigVerificationResult;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaToS3IT {

    private static ConnectorTestRunner runner;
    private static Network network;
    private static ConfluentKafkaContainer kafkaContainer;
    private static GenericContainer<?> schemaRegistryContainer;
    private static LocalStackContainer localStackContainer;
    private static S3Client s3Client;

    private static final String SCRAM_USERNAME = "testuser";
    private static final String SCRAM_PASSWORD = "testpassword";

    private static final String S3_REGION = "us-west-2";

    // JAAS configuration for Kafka broker SASL/PLAIN authentication.
    // The 'username' and 'password' fields are credentials the broker uses for inter-broker communication.
    // The 'user_<username>="<password>"' entries define client users that can authenticate to this broker.
    // In this setup:
    //   - Broker uses 'admin' / 'admin-secret' for inter-broker communication (though we use PLAINTEXT for that)
    //   - Clients can authenticate using 'testuser' / 'testpassword' on the SASL listener with PLAIN mechanism
    private static final String JAAS_CONFIG_CONTENT = """
        KafkaServer {
          org.apache.kafka.common.security.plain.PlainLoginModule required
          username="admin"
          password="admin-secret"
          user_%s="%s";
        };
        """.formatted(SCRAM_USERNAME, SCRAM_PASSWORD);


    @BeforeAll
    public static void setupTestContainers() {
        network = Network.newNetwork();

        kafkaContainer = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withStartupTimeout(Duration.ofSeconds(10))
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT")
            .withEnv("KAFKA_LISTENERS", "CONTROLLER://0.0.0.0:9094,BROKER://0.0.0.0:9092,PLAINTEXT://0.0.0.0:19092,SASL://0.0.0.0:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://kafka:9092,PLAINTEXT://kafka:19092,SASL://localhost:9093")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
            .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/kafka_jaas.conf")
            .withCommand(
                "sh", "-c",
                "echo '" + JAAS_CONFIG_CONTENT + "' > /tmp/kafka_jaas.conf && " +
                "/etc/confluent/docker/run"
            );

        kafkaContainer.setPortBindings(List.of("9093:9093"));
        kafkaContainer.start();

        schemaRegistryContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.8.0"))
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withStartupTimeout(Duration.ofSeconds(60))
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .dependsOn(kafkaContainer);

        schemaRegistryContainer.start();

        localStackContainer = new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.1.0"))
            .withServices(LocalStackContainer.Service.S3)
            .withStartupTimeout(Duration.ofSeconds(30));

        localStackContainer.start();

        s3Client = S3Client.builder()
            .endpointOverride(localStackContainer.getEndpoint())
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
            .region(Region.of(S3_REGION))
            .httpClient(UrlConnectionHttpClient.builder().build())
            .forcePathStyle(true)
            .build();
    }

    @BeforeEach
    public void setupRunner() {
        runner = new StandardConnectorTestRunner.Builder()
            .connectorClassName("org.apache.nifi.connectors.kafkas3.KafkaToS3")
            .narLibraryDirectory(new File("target/libDir"))
            .build();
        assertNotNull(runner);
    }

    @AfterAll
    public static void cleanupTestContainers() {
        if (s3Client != null) {
            s3Client.close();
        }

        if (localStackContainer != null) {
            localStackContainer.stop();
        }

        if (schemaRegistryContainer != null) {
            schemaRegistryContainer.stop();
        }

        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }

        if (network != null) {
            network.close();
        }
    }

    @AfterEach
    public void cleanupRunner() throws IOException {
        if (runner != null) {
            runner.close();
        }
    }


    private void createS3Bucket(final String bucketName) {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    private void createKafkaTopics(final String... topicNames) throws ExecutionException, InterruptedException {
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        adminProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        adminProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            SCRAM_USERNAME, SCRAM_PASSWORD
        ));

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            final List<NewTopic> topics = new ArrayList<>();
            for (final String topicName : topicNames) {
                topics.add(new NewTopic(topicName, 1, (short) 1));
            }

            adminClient.createTopics(topics).all().get();
        }
    }

    private void produceRecordsToTopic(final String topicName, final String... records) throws ExecutionException, InterruptedException {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            SCRAM_USERNAME, SCRAM_PASSWORD
        ));

        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (final String record : records) {
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, record);
                producer.send(producerRecord).get();
            }

            producer.flush();
        }
    }

    private String getSchemaRegistryUrl() {
        return String.format("http://%s:%d", schemaRegistryContainer.getHost(), schemaRegistryContainer.getMappedPort(8081));
    }

    private void produceAvroRecordsToTopic(final String topicName, final Schema schema, final GenericRecord... records) throws ExecutionException, InterruptedException {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            SCRAM_USERNAME, SCRAM_PASSWORD
        ));
        producerProps.put("schema.registry.url", getSchemaRegistryUrl());

        try (final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProps)) {
            for (final GenericRecord record : records) {
                final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topicName, record);
                producer.send(producerRecord).get();
            }

            producer.flush();
        }
    }


    @Test
    public void testVerification() throws ExecutionException, InterruptedException, FlowUpdateException {
        createKafkaTopics("topic-1", "topic-2", "topic-3", "topic-4", "topic-5", "Z-topic", "an-important-topic");

        produceRecordsToTopic("topic-1",
            """
            {"id": 1, "name": "Alice", "age": 30}""",
            """
            {"id": 2, "name": "Bob", "age": 25}""",
            """
            {"id": 3, "name": "Charlie", "age": 35}"""
        );

        produceRecordsToTopic("an-important-topic",
            "This is a plaintext message",
            "Another important message",
            "Final plaintext record"
        );

        final Map<String, String> kafkaServerConfig = Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        );

        runner.applyUpdate();

        // Perform verification to ensure that valid server configuration passes
        final ConnectorConfigVerificationResult connectionVerificationResults = runner.verifyConfiguration("Kafka Connection", kafkaServerConfig);
        connectionVerificationResults.assertNoFailures();

        // Apply the configuration that we've now validated
        runner.configure("Kafka Connection", kafkaServerConfig);

        // Perform verification to ensure that valid topic configuration passes
        final Map<String, String> topic1Config = Map.of(
            "Topic Names", "topic-1",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        );
        final ConnectorConfigVerificationResult topic1VerificationResults = runner.verifyConfiguration("Kafka Topics", topic1Config);
        topic1VerificationResults.assertNoFailures();

        // Perform verification against a topic with invalid data for the selected data format
        final Map<String, String> importantTopicConfig = Map.of(
            "Topic Names", "an-important-topic",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        );

        final ConnectorConfigVerificationResult importantTopicVerificationResults = runner.verifyConfiguration("Kafka Topics", importantTopicConfig);
        final List<ConfigVerificationResult> invalidImportantTopicResults = importantTopicVerificationResults.getFailedResults();
        assertEquals(1, invalidImportantTopicResults.size());
        final ConfigVerificationResult invalidResult = invalidImportantTopicResults.getFirst();
        assertTrue(invalidResult.getExplanation().contains("parse"), "Unexpected validation reason: " + invalidResult.getExplanation());

        runner.applyUpdate();
    }

    @Test
    public void testJsonFlow() throws IOException, ExecutionException, InterruptedException, FlowUpdateException {
        final String bucketName = "test-json-flow";
        createS3Bucket(bucketName);
        createKafkaTopics("story");

        produceRecordsToTopic("story",
            """
            {"page": 1, "words": "Once upon a time, there was a NiFi developer." }""",
            """
            {"page": 2, "words": "The developer wanted to build a connector to move data from Kafka to S3." }""",
            """
            {"page": 3, "words": "After much effort, the connector was complete and worked flawlessly!" }""",
            """
            {"page": 4, "words": "The end." }"""
        );

        final Map<String, String> kafkaServerConfig = Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        );

        final Map<String, String> kafkaTopicConfig = Map.of(
            "Topic Names", "story",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        );

        final Map<String, String> s3Config = Map.ofEntries(
            Map.entry("S3 Region", S3_REGION),
            Map.entry("S3 Data Format", "Avro"),
            Map.entry("S3 Bucket", bucketName),
            Map.entry("S3 Endpoint Override URL", localStackContainer.getEndpoint().toString()),
            Map.entry("S3 Authentication Strategy", "Access Key ID and Secret Key"),
            Map.entry("S3 Access Key ID", localStackContainer.getAccessKey()),
            Map.entry("S3 Secret Access Key", localStackContainer.getSecretKey()),
            Map.entry("Target Object Size", "1 MB"),
            Map.entry("Merge Latency", "1 sec")
        );

        runner.configure("Kafka Connection", kafkaServerConfig);
        runner.configure("Kafka Topics", kafkaTopicConfig);
        runner.configure("S3 Configuration", s3Config);
        runner.applyUpdate();

        final List<ValidationResult> validationResults = runner.validate();
        assertEquals(Collections.emptyList(), validationResults);

        runner.startConnector();
        try {
            runner.waitForDataIngested(Duration.ofSeconds(10));
            runner.waitForIdle(Duration.ofSeconds(30));
        } finally {
            runner.stopConnector();
        }

        verifyS3ObjectsCreated(bucketName);
    }

    private void verifyS3ObjectsCreated(final String bucketName) throws IOException {
        final ListObjectsV2Response listResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
        final List<S3Object> objects = listResponse.contents();

        assertFalse(objects.isEmpty(), "Expected at least one object in S3 bucket");

        for (final S3Object s3Object : objects) {
            final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();
            try (final ResponseInputStream<GetObjectResponse> objectContent = s3Client.getObject(getObjectRequest)) {
                final long objectSize = objectContent.response().contentLength();
                assertTrue(objectSize > 0, "Expected S3 object " + s3Object.key() + " to have content");
            }
        }
    }

    @Test
    public void testSchemaRegistryVerification() throws ExecutionException, InterruptedException, FlowUpdateException {
        createKafkaTopics("avro-topic");

        final String schemaString = """
            {
              "type": "record",
              "name": "TestRecord",
              "namespace": "org.apache.nifi.test",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "message", "type": "string"}
              ]
            }""";

        final Schema schema = new Schema.Parser().parse(schemaString);

        final GenericRecord record1 = new GenericData.Record(schema);
        record1.put("id", 100);
        record1.put("message", "Test message 1");

        final GenericRecord record2 = new GenericData.Record(schema);
        record2.put("id", 200);
        record2.put("message", "Test message 2");

        produceAvroRecordsToTopic("avro-topic", schema, record1, record2);

        final Map<String, String> kafkaConnectionConfig = Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD,
            "Schema Registry URL", getSchemaRegistryUrl()
        );

        final ConnectorConfigVerificationResult connectionVerificationResults = runner.verifyConfiguration("Kafka Connection", kafkaConnectionConfig);
        connectionVerificationResults.assertNoFailures();

        runner.configure("Kafka Connection", kafkaConnectionConfig);

        final Map<String, String> avroTopicConfig = Map.of(
            "Topic Names", "avro-topic",
            "Consumer Group ID", "nifi-kafka-to-s3-testSchemaRegistryVerification",
            "Offset Reset", "earliest",
            "Kafka Data Format", "Avro"
        );

        final ConnectorConfigVerificationResult avroTopicVerificationResults = runner.verifyConfiguration("Kafka Topics", avroTopicConfig);
        avroTopicVerificationResults.assertNoFailures();

        runner.applyUpdate();
    }

    @Test
    public void testWithSchemaRegistry() throws IOException, ExecutionException, InterruptedException, FlowUpdateException {
        final String bucketName = "test-schema-registry";
        createS3Bucket(bucketName);
        createKafkaTopics("user-events");

        final String schemaString = """
            {
              "type": "record",
              "name": "UserEvent",
              "namespace": "org.apache.nifi.test",
              "fields": [
                {"name": "userId", "type": "int"},
                {"name": "userName", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "timestamp", "type": "long"}
              ]
            }""";

        final Schema schema = new Schema.Parser().parse(schemaString);

        final GenericRecord record1 = new GenericData.Record(schema);
        record1.put("userId", 1001);
        record1.put("userName", "alice");
        record1.put("eventType", "login");
        record1.put("timestamp", System.currentTimeMillis());

        final GenericRecord record2 = new GenericData.Record(schema);
        record2.put("userId", 1002);
        record2.put("userName", "bob");
        record2.put("eventType", "purchase");
        record2.put("timestamp", System.currentTimeMillis());

        final GenericRecord record3 = new GenericData.Record(schema);
        record3.put("userId", 1003);
        record3.put("userName", "charlie");
        record3.put("eventType", "logout");
        record3.put("timestamp", System.currentTimeMillis());

        produceAvroRecordsToTopic("user-events", schema, record1, record2, record3);

        final Map<String, String> kafkaConnectionConfig = Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD,
            "Schema Registry URL", getSchemaRegistryUrl()
        );

        final Map<String, String> kafkaTopicConfig = Map.of(
            "Topic Names", "user-events",
            "Consumer Group ID", "nifi-kafka-to-s3-testSchemaRegistry",
            "Offset Reset", "earliest",
            "Kafka Data Format", "Avro"
        );

        final Map<String, String> s3Config = Map.ofEntries(
            Map.entry("S3 Region", S3_REGION),
            Map.entry("S3 Data Format", "Avro"),
            Map.entry("S3 Bucket", bucketName),
            Map.entry("S3 Endpoint Override URL", localStackContainer.getEndpoint().toString()),
            Map.entry("S3 Authentication Strategy", "Access Key ID and Secret Key"),
            Map.entry("S3 Access Key ID", localStackContainer.getAccessKey()),
            Map.entry("S3 Secret Access Key", localStackContainer.getSecretKey()),
            Map.entry("Target Object Size", "1 MB"),
            Map.entry("Merge Latency", "1 sec")
        );

        runner.configure("Kafka Connection", kafkaConnectionConfig);
        runner.configure("Kafka Topics", kafkaTopicConfig);
        runner.configure("S3 Configuration", s3Config);
        runner.applyUpdate();

        final List<ValidationResult> validationResults = runner.validate();
        assertEquals(Collections.emptyList(), validationResults);

        runner.startConnector();
        try {
            runner.waitForDataIngested(Duration.ofSeconds(10));
            runner.waitForIdle(Duration.ofSeconds(30));
        } finally {
            runner.stopConnector();
        }

        verifyS3ObjectsCreated(bucketName);
    }

    @Test
    public void testSwitchFromJsonToAvroS3Format() throws IOException, ExecutionException, InterruptedException, FlowUpdateException {
        final String bucketName = "test-switch-formats";
        createS3Bucket(bucketName);

        // Add data to json and avro topics
        createKafkaTopics("json", "avro");

        produceRecordsToTopic("json",
            """
            {"id": 1, "type": "json", "data": "First JSON record"}""",
            """
            {"id": 2, "type": "json", "data": "Second JSON record"}""",
            """
            {"id": 3, "type": "json", "data": "Third JSON record"}"""
        );

        final String schemaString = """
            {
              "type": "record",
              "name": "AvroRecord",
              "namespace": "org.apache.nifi.test",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "type", "type": "string"},
                {"name": "data", "type": "string"}
              ]
            }""";

        final Schema schema = new Schema.Parser().parse(schemaString);

        final GenericRecord avroRecord1 = new GenericData.Record(schema);
        avroRecord1.put("id", 10);
        avroRecord1.put("type", "avro");
        avroRecord1.put("data", "First Avro record");

        final GenericRecord avroRecord2 = new GenericData.Record(schema);
        avroRecord2.put("id", 20);
        avroRecord2.put("type", "avro");
        avroRecord2.put("data", "Second Avro record");

        final GenericRecord avroRecord3 = new GenericData.Record(schema);
        avroRecord3.put("id", 30);
        avroRecord3.put("type", "avro");
        avroRecord3.put("data", "Third Avro record");

        produceAvroRecordsToTopic("avro", schema, avroRecord1, avroRecord2, avroRecord3);

        // Configure Connector to consume from JSON Kafka topic and write to S3 in JSON format, but with an invalid S3 endpoint.
        // This will cause the data to remain queued, since PutS3Object will fail to write the data.
        final Map<String, String> kafkaServerConfig = Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        );

        final Map<String, String> jsonTopicConfig = Map.of(
            "Topic Names", "json",
            "Consumer Group ID", "nifi-kafka-to-s3-testReconfiguration",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        );

        final Map<String, String> s3InvalidConfig = Map.ofEntries(
            Map.entry("S3 Region", S3_REGION),
            Map.entry("S3 Data Format", "JSON"),
            Map.entry("S3 Bucket", bucketName),
            Map.entry("S3 Endpoint Override URL", "http://invalid-s3-endpoint:9999"),
            Map.entry("S3 Authentication Strategy", "Access Key ID and Secret Key"),
            Map.entry("S3 Access Key ID", localStackContainer.getAccessKey()),
            Map.entry("S3 Secret Access Key", localStackContainer.getSecretKey()),
            Map.entry("Target Object Size", "1 MB"),
            Map.entry("Merge Latency", "1 sec")
        );

        runner.configure("Kafka Connection", kafkaServerConfig);
        runner.configure("Kafka Topics", jsonTopicConfig);
        runner.configure("S3 Configuration", s3InvalidConfig);
        runner.applyUpdate();

        // Run the Connector with the invalid S3 endpoint to queue the JSON data. Wait for data to be queued up.
        runner.startConnector();
        try {
            runner.waitForDataIngested(Duration.ofSeconds(30));
        } finally {
            runner.stopConnector();
        }

        // Apply configuration to specify the correct S3 endpoint. Keep S3 Data Format as JSON for now.
        // This will allow the Connector to write data to S3 and properly drain the data when we switch the S3 format from JSON to Avro.
        final Map<String, String> s3ValidJsonConfig = Map.ofEntries(
            Map.entry("S3 Region", S3_REGION),
            Map.entry("S3 Data Format", "JSON"),
            Map.entry("S3 Bucket", bucketName),
            Map.entry("S3 Endpoint Override URL", localStackContainer.getEndpoint().toString()),
            Map.entry("S3 Authentication Strategy", "Access Key ID and Secret Key"),
            Map.entry("S3 Access Key ID", localStackContainer.getAccessKey()),
            Map.entry("S3 Secret Access Key", localStackContainer.getSecretKey()),
            Map.entry("Target Object Size", "1 MB"),
            Map.entry("Merge Latency", "1 sec")
        );

        runner.configure("S3 Configuration", s3ValidJsonConfig);
        runner.applyUpdate();

        // Make sure there is no data in S3 yet.
        final ListObjectsV2Response initialListingResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
        final List<S3Object> initialS3Objects = initialListingResponse.contents();
        assertEquals(List.of(), initialS3Objects);

        // Now change the S3 Data Format from JSON to Avro. This should trigger draining of the queued JSON data.
        final Map<String, String> s3ValidAvroConfig = Map.ofEntries(
            Map.entry("S3 Region", S3_REGION),
            Map.entry("S3 Data Format", "Avro"),
            Map.entry("S3 Bucket", bucketName),
            Map.entry("S3 Endpoint Override URL", localStackContainer.getEndpoint().toString()),
            Map.entry("S3 Authentication Strategy", "Access Key ID and Secret Key"),
            Map.entry("S3 Access Key ID", localStackContainer.getAccessKey()),
            Map.entry("S3 Secret Access Key", localStackContainer.getSecretKey()),
            Map.entry("Target Object Size", "1 MB"),
            Map.entry("Merge Latency", "1 sec")
        );

        // Configure to consume from the Avro topic and change S3 format to Avro
        final Map<String, String> avroTopicConfig = Map.of(
            "Topic Names", "avro",
            "Consumer Group ID", "nifi-kafka-to-s3-testReconfiguration",
            "Offset Reset", "earliest",
            "Kafka Data Format", "Avro"
        );

        final Map<String, String> kafkaConnectionWithSchemaRegistry = Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD,
            "Schema Registry URL", getSchemaRegistryUrl()
        );

        runner.configure("Kafka Connection", kafkaConnectionWithSchemaRegistry);
        runner.configure("Kafka Topics", avroTopicConfig);
        runner.configure("S3 Configuration", s3ValidAvroConfig);
        runner.applyUpdate();

        // After draining, there should be one JSON file in S3.
        final ListObjectsV2Response jsonOnlyListing = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
        final List<S3Object> jsonObjects = jsonOnlyListing.contents();
        assertEquals(1, jsonObjects.size());

        // Start connector, wait for more data to be ingested, and then wait for the Connector to be idle.
        runner.startConnector();
        try {
            runner.waitForDataIngested(Duration.ofSeconds(30));
            runner.waitForIdle(Duration.ofSeconds(30));
        } finally {
            runner.stopConnector();
        }

        // Verify that there are two objects in S3: one in JSON format and one in Avro format.
        final ListObjectsV2Response listResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
        final List<S3Object> objects = listResponse.contents();

        assertEquals(2, objects.size(), "Expected exactly 2 objects in S3 bucket: one for JSON data and one for Avro data");

        for (final S3Object s3Object : objects) {
            final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();
            try (final ResponseInputStream<GetObjectResponse> objectContent = s3Client.getObject(getObjectRequest)) {
                final long objectSize = objectContent.response().contentLength();
                assertTrue(objectSize > 0, "Expected S3 object " + s3Object.key() + " to have content");
            }
        }
    }

}
