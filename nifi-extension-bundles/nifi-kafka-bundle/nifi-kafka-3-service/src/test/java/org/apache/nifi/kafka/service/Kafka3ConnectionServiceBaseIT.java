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
package org.apache.nifi.kafka.service;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.producer.RecordSummary;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(Lifecycle.PER_CLASS)
public class Kafka3ConnectionServiceBaseIT {
    // This Base class executes its tests with Ssl off and Sasl off.
    // There are subclasses which execute these same tests and enable Ssl or Sasl

    public static final String IMAGE_NAME = "confluentinc/cp-kafka:7.8.0";  // December 2024

    private static final String GROUP_ID = Kafka3ConnectionService.class.getSimpleName();

    private static final String TOPIC = Kafka3ConnectionServiceBaseIT.class.getSimpleName();

    private static final String SERVICE_ID = Kafka3ConnectionService.class.getSimpleName();

    private static final String UNREACHABLE_BOOTSTRAP_SERVERS = "127.0.0.1:1000";

    private static final String CLIENT_TIMEOUT = "2 s";

    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    private static final byte[] RECORD_KEY = new byte[]{1};

    private static final byte[] RECORD_VALUE = TEST_RECORD_VALUE.getBytes(StandardCharsets.UTF_8);

    private static final int POLLING_ATTEMPTS = 3;

    private static final Set<String> fileLocationNames = Set.of("KAFKA_SSL_KEYSTORE_LOCATION", "KAFKA_SSL_TRUSTSTORE_LOCATION");

    protected static final String TEST_USERNAME = "nifi";
    protected static final String TEST_PASSWORD = UUID.randomUUID().toString();

    private static final String KEY_STORE_EXTENSION = ".p12";

    protected static final String KEY_PASSWORD = Kafka3ConnectionServiceBaseIT.class.getSimpleName();

    protected static final String KEY_STORE_PASSWORD = KEY_PASSWORD;

    @TempDir
    private static Path keyStoreDirectory;

    protected static Path keyStorePath;

    protected static String keyStoreType;

    protected static Path trustStorePath;

    protected TestRunner runner;

    private ConfluentKafkaContainer kafkaContainer;

    private Kafka3ConnectionService service;

    @BeforeAll
    void startContainer() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .keyPassword(KEY_PASSWORD.toCharArray())
                .build();

        keyStorePath = File.createTempFile("keyStore", KEY_STORE_EXTENSION, keyStoreDirectory.toFile()).toPath();
        try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, KEY_STORE_PASSWORD.toCharArray());
        }
        keyStoreType = keyStore.getType().toUpperCase();

        final KeyStore trustStore = new EphemeralKeyStoreBuilder()
                .addCertificate(certificate)
                .build();
        trustStorePath = File.createTempFile("trustStore", KEY_STORE_EXTENSION, keyStoreDirectory.toFile()).toPath();
        try (OutputStream outputStream = Files.newOutputStream(trustStorePath)) {
            trustStore.store(outputStream, KEY_STORE_PASSWORD.toCharArray());
        }

        kafkaContainer = new ConfluentKafkaContainer(DockerImageName.parse(IMAGE_NAME));
        initializeContainer();
        kafkaContainer.start();
    }

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new Kafka3ConnectionService();
        runner.addControllerService(SERVICE_ID, service, getKafkaServiceConfigProperties());
        runner.enableControllerService(service);
    }

    @AfterEach
    void cleanup() {
        runner.disableControllerService(service);
    }

    @AfterAll
    void stopContainer() {
        kafkaContainer.stop();
    }

    protected void initializeContainer() {
        Map<String, String> environment = getKafkaContainerConfigProperties();
        kafkaContainer.withEnv(environment);

        // For each property which is a file location, copy the file into the kafka container.
        environment.entrySet().stream()
                .filter(e -> fileLocationNames.contains(e.getKey()))
                .forEach(e -> kafkaContainer.withCopyFileToContainer(
                        MountableFile.forHostPath(e.getValue()), e.getValue()));
    }

    protected Map<String, String> getKafkaContainerConfigProperties() {
        return Collections.emptyMap();
    }

    protected Map<String, String> getKafkaServiceConfigProperties() throws InitializationException {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put(Kafka3ConnectionService.BOOTSTRAP_SERVERS.getName(), kafkaContainer.getBootstrapServers());
        properties.put(Kafka3ConnectionService.CLIENT_TIMEOUT.getName(), CLIENT_TIMEOUT);
        return properties;
    }

    protected Map<String, String> getAdminClientConfigProperties() {
        return Collections.emptyMap();
    }

    @Test
    void testAdminClient() throws ExecutionException, InterruptedException, TimeoutException {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        properties.putAll(getAdminClientConfigProperties());

        try (final Admin adminClient = Admin.create(properties)) {
            final int numPartitions = 1;
            final short replicationFactor = 1;
            final NewTopic newTopic = new NewTopic(TOPIC, numPartitions, replicationFactor);
            final CreateTopicsResult topics = adminClient.createTopics(Collections.singleton(newTopic));
            final KafkaFuture<Void> topicFuture = topics.values().get(TOPIC);
            topicFuture.get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void testProduceOneNoTransaction() {
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(false, null, null, null, null);
        final KafkaProducerService producerService = service.getProducerService(producerConfiguration);
        final KafkaRecord kafkaRecord = new KafkaRecord(null, null, null, null, RECORD_VALUE, Collections.emptyList());
        final List<KafkaRecord> kafkaRecords = Collections.singletonList(kafkaRecord);
        producerService.send(kafkaRecords.iterator(), new PublishContext(TOPIC + "-produce", null, null, null));
        final RecordSummary summary = producerService.complete();
        assertNotNull(summary);
    }

    @Test
    void testProduceOneWithTransaction() {
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(true, "transaction-", null, null, null);
        final KafkaProducerService producerService = service.getProducerService(producerConfiguration);
        final KafkaRecord kafkaRecord = new KafkaRecord(null, null, null, null, RECORD_VALUE, Collections.emptyList());
        final List<KafkaRecord> kafkaRecords = Collections.singletonList(kafkaRecord);
        producerService.send(kafkaRecords.iterator(), new PublishContext(TOPIC + "-produce", null, null, null));
        final RecordSummary summary = producerService.complete();
        assertNotNull(summary);
    }

    @Test
    void testProduceConsumeRecord() {
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(false, null, null, null, null);
        final KafkaProducerService producerService = service.getProducerService(producerConfiguration);

        final long timestamp = System.currentTimeMillis();
        final KafkaRecord kafkaRecord = new KafkaRecord(null, null, timestamp, RECORD_KEY, RECORD_VALUE, Collections.emptyList());
        final List<KafkaRecord> kafkaRecords = Collections.singletonList(kafkaRecord);
        producerService.send(kafkaRecords.iterator(), new PublishContext(TOPIC, null, null, null));
        final RecordSummary summary = producerService.complete();
        assertNotNull(summary);

        final PollingContext pollingContext = new PollingContext(GROUP_ID, Collections.singleton(TOPIC), AutoOffsetReset.EARLIEST, Duration.ofSeconds(1));
        final KafkaConsumerService consumerService = service.getConsumerService(pollingContext);
        final Iterator<ByteRecord> consumerRecords = poll(consumerService);

        assertTrue(consumerRecords.hasNext(), "Consumer Records not found");

        final ByteRecord consumerRecord = consumerRecords.next();
        assertEquals(TOPIC, consumerRecord.getTopic());
        assertEquals(0, consumerRecord.getOffset());
        assertEquals(0, consumerRecord.getPartition());
        assertEquals(timestamp, consumerRecord.getTimestamp());

        final Optional<byte[]> keyFound = consumerRecord.getKey();
        assertTrue(keyFound.isPresent());

        assertArrayEquals(RECORD_KEY, keyFound.get());
        assertArrayEquals(RECORD_VALUE, consumerRecord.getValue());

        assertFalse(consumerRecords.hasNext());
    }

    @Test
    void testVerifySuccessful() {
        final Map<PropertyDescriptor, String> properties = new LinkedHashMap<>();
        properties.put(Kafka3ConnectionService.BOOTSTRAP_SERVERS, kafkaContainer.getBootstrapServers());
        final MockConfigurationContext configurationContext = new MockConfigurationContext(properties, null, null);

        final List<ConfigVerificationResult> results = service.verify(
                configurationContext, runner.getLogger(), getAdminClientConfigProperties());

        assertFalse(results.isEmpty());

        final ConfigVerificationResult firstResult = results.iterator().next();
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, firstResult.getOutcome());
        assertNotNull(firstResult.getExplanation());
    }

    @Test
    void testVerifyFailed() {
        final Map<PropertyDescriptor, String> properties = new LinkedHashMap<>();
        properties.put(Kafka3ConnectionService.BOOTSTRAP_SERVERS, UNREACHABLE_BOOTSTRAP_SERVERS);
        properties.put(Kafka3ConnectionService.CLIENT_TIMEOUT, CLIENT_TIMEOUT);

        final MockConfigurationContext configurationContext = new MockConfigurationContext(
                properties, null, null);

        final List<ConfigVerificationResult> results = service.verify(
                configurationContext, runner.getLogger(), getAdminClientConfigProperties());

        assertFalse(results.isEmpty());

        final ConfigVerificationResult firstResult = results.iterator().next();
        assertEquals(ConfigVerificationResult.Outcome.FAILED, firstResult.getOutcome());
    }

    @Test
    void testGetProducerService() {
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(false, null, null, null, null);
        final KafkaProducerService producerService = service.getProducerService(producerConfiguration);
        final List<PartitionState> partitionStates = producerService.getPartitionStates(TOPIC);
        assertPartitionStatesFound(partitionStates);
    }

    @Test
    void testGetConsumerService() {
        final PollingContext pollingContext = new PollingContext(GROUP_ID, Collections.singleton(TOPIC), AutoOffsetReset.EARLIEST, Duration.ofSeconds(1));
        final KafkaConsumerService consumerService = service.getConsumerService(pollingContext);
        final List<PartitionState> partitionStates = consumerService.getPartitionStates();
        assertPartitionStatesFound(partitionStates);
    }

    private void assertPartitionStatesFound(final List<PartitionState> partitionStates) {
        assertEquals(1, partitionStates.size());
        final PartitionState partitionState = partitionStates.iterator().next();
        assertEquals(TOPIC, partitionState.getTopic());
        assertEquals(0, partitionState.getPartition());
    }

    private Iterator<ByteRecord> poll(final KafkaConsumerService consumerService) {
        Iterator<ByteRecord> consumerRecords = Collections.emptyIterator();

        for (int i = 0; i < POLLING_ATTEMPTS; i++) {
            final Iterable<ByteRecord> records = consumerService.poll();
            assertNotNull(records);
            consumerRecords = records.iterator();
            if (consumerRecords.hasNext()) {
                break;
            }
        }

        return consumerRecords;
    }

    protected String addSSLContextService(final TestRunner runner) throws InitializationException {
        final String identifier = SSLContextService.class.getSimpleName();
        final SSLContextService service = mock(SSLContextService.class);
        when(service.getIdentifier()).thenReturn(identifier);
        runner.addControllerService(identifier, service);

        when(service.isKeyStoreConfigured()).thenReturn(true);
        when(service.getKeyStoreFile()).thenReturn(keyStorePath.toString());
        when(service.getKeyStoreType()).thenReturn(keyStoreType);
        when(service.getKeyStorePassword()).thenReturn(KEY_STORE_PASSWORD);
        when(service.isTrustStoreConfigured()).thenReturn(true);
        when(service.getTrustStoreFile()).thenReturn(trustStorePath.toString());
        when(service.getTrustStoreType()).thenReturn(keyStoreType);
        when(service.getTrustStorePassword()).thenReturn(KEY_STORE_PASSWORD);

        runner.enableControllerService(service);
        return identifier;
    }

    protected String getJaasConfigKafkaContainer(String userName, String password) {
        return String.format("%s required user_%s=\"%s\";",
                PlainLoginModule.class.getName(), userName, password);
    }

    protected String getJaasConfigKafkaClient(String userName, String password) {
        return String.format("%s required username=\"%s\" password=\"%s\";",
                PlainLoginModule.class.getName(), userName, password);
    }
}
