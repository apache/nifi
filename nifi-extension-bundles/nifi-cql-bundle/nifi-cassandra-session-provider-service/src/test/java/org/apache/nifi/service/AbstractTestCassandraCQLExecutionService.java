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

package org.apache.nifi.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.nifi.cassandra.api.CqlQueryCallback;
import org.apache.nifi.cassandra.api.UpdateMethod;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.service.mock.MockCassandraProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
public abstract class AbstractTestCassandraCQLExecutionService {
    public static final String adminPassword = UUID.randomUUID().toString();

    private static TestRunner runner;
    private static CassandraCQLExecutionService sessionProvider;

    private static final Map<String, String> CONTAINER_ENVIRONMENT = new LinkedHashMap<>();

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    private static final X500Principal CERTIFICATE_ISSUER = new X500Principal("CN=localhost");

    private static final Collection<String> DNS_NAMES = Collections.singleton("localhost");

    private static final String CERTIFICATE_FORMAT = "-----BEGIN CERTIFICATE-----%n%s%n-----END CERTIFICATE-----";

    private static final String KEY_FORMAT = "-----BEGIN PRIVATE KEY-----%n%s%n-----END PRIVATE KEY-----";

    private static final String SSL_DIRECTORY = "/ssl";

    private static final String CERTIFICATE_FILE = "public.crt";

    private static final String CONTAINER_CERTIFICATE_PATH = String.format("%s/%s", SSL_DIRECTORY, CERTIFICATE_FILE);

    private static final String KEY_FILE = "private.key";

    private static final String CONTAINER_KEY_PATH = String.format("%s/%s", SSL_DIRECTORY, KEY_FILE);

    public static CassandraContainer container;

    public static final String KEYSPACE = "testspace";

    static CqlSession session;

    public static void setup(String cassandraDockerString) throws Exception {
        container = new CassandraContainer(cassandraDockerString)
                .withInitScript("init.cql");

        setCertificatePrivateKey();

        container.withEnv(CONTAINER_ENVIRONMENT);
        container.withExposedPorts(9042);
        container.start();

        MockCassandraProcessor mockCassandraProcessor = new MockCassandraProcessor();
        sessionProvider = new CassandraCQLExecutionService();

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);

        runner = TestRunners.newTestRunner(mockCassandraProcessor);
        runner.addControllerService("cassandra-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CassandraCQLExecutionService.USERNAME, "admin");
        runner.setProperty(sessionProvider, CassandraCQLExecutionService.PASSWORD, adminPassword);
        runner.setProperty(sessionProvider, CassandraCQLExecutionService.CONTACT_POINTS, contactPoint);
        runner.setProperty(sessionProvider, CassandraCQLExecutionService.DATACENTER, "datacenter1");
        runner.setProperty(sessionProvider, CassandraCQLExecutionService.KEYSPACE, KEYSPACE);

        runner.enableControllerService(sessionProvider);

        session = CqlSession
                .builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter("datacenter1")
                .build();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        container.stop();
    }

    private RecordSchema getSchema() {
        List<RecordField> fields = List.of(
                new RecordField("sender", RecordFieldType.STRING.getDataType()),
                new RecordField("receiver", RecordFieldType.STRING.getDataType()),
                new RecordField("message", RecordFieldType.STRING.getDataType()),
                new RecordField("when_sent", RecordFieldType.TIMESTAMP.getDataType())
        );
        return new SimpleRecordSchema(fields);
    }

    @Test
    public void testInsertRecord() {
        RecordSchema schema = getSchema();
        Map<String, Object> rawRecord = new HashMap<>();
        rawRecord.put("sender", "john.smith");
        rawRecord.put("receiver", "jane.smith");
        rawRecord.put("message", "hello");
        rawRecord.put("when_sent", Instant.now());

        MapRecord record = new MapRecord(schema, rawRecord);

        assertDoesNotThrow(() -> sessionProvider.insert("message", record));
    }

    @Test
    public void testIncrementAndDecrement() throws Exception {
        RecordField field1 = new RecordField("column_a", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("increment_field", RecordFieldType.INT.getDataType());
        RecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        HashMap<String, Object> map = new HashMap<>();
        map.put("column_a", "abcdef");
        map.put("increment_field", 1);

        MapRecord record = new MapRecord(schema, map);

        List<String> updateKeys = new ArrayList<>();
        updateKeys.add("column_a");

        //Set the initial value
        sessionProvider.update("counter_test", record, updateKeys, UpdateMethod.INCREMENT);

        Thread.sleep(1000);

        sessionProvider.update("counter_test", record, updateKeys, UpdateMethod.INCREMENT);

        ResultSet results = session.execute("select increment_field from testspace.counter_test where column_a = 'abcdef'");

        Iterator<Row> rowIterator = results.iterator();

        Row row = rowIterator.next();

        assertEquals(2, row.getLong("increment_field"));

        sessionProvider.update("counter_test", record, updateKeys, UpdateMethod.DECREMENT);

        results = session.execute("select increment_field from testspace.counter_test where column_a = 'abcdef'");

        rowIterator = results.iterator();

        row = rowIterator.next();

        assertEquals(1, row.getLong("increment_field"));
    }

    @Test
    public void testUpdateSet() throws Exception {
        session.execute("insert into testspace.simple_set_test(username, is_active) values('john.smith', true)");
        Thread.sleep(250);

        RecordField field1 = new RecordField("username", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("is_active", RecordFieldType.BOOLEAN.getDataType());
        RecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        HashMap<String, Object> map = new HashMap<>();
        map.put("username", "john.smith");
        map.put("is_active", false);

        MapRecord record = new MapRecord(schema, map);

        List<String> updateKeys = new ArrayList<>();
        updateKeys.add("username");

        sessionProvider.update("simple_set_test", record, updateKeys, UpdateMethod.SET);

        Iterator<Row> iterator = session.execute("select is_active from testspace.simple_set_test where username = 'john.smith'").iterator();

        Row row = iterator.next();

        assertFalse(row.getBoolean("is_active"));
    }

    @Test
    public void testQueryRecord() {
        String[] statements = """
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'def', toTimestamp(now()));
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'ghi', toTimestamp(now()));
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'jkl', toTimestamp(now()));
        """.trim().split("\\;");
        for (String statement : statements) {
            session.execute(statement);
        }

        List<org.apache.nifi.serialization.record.Record> records = new ArrayList<>();
        CqlQueryCallback callback = (rowNumber, result, fields, isExhausted) -> records.add(result);

        sessionProvider.query("select * from testspace.query_test", false, null, callback);
    }

    private static void setCertificatePrivateKey() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, CERTIFICATE_ISSUER, Duration.ofDays(1))
                .setDnsSubjectAlternativeNames(DNS_NAMES)
                .build();

        final Key key = keyPair.getPrivate();
        final String keyEncoded = getKeyEncoded(key);
        container.withCopyToContainer(Transferable.of(keyEncoded), CONTAINER_KEY_PATH);

        final String certificateEncoded = getCertificateEncoded(certificate);
        container.withCopyToContainer(Transferable.of(certificateEncoded), CONTAINER_CERTIFICATE_PATH);
        writeCertificateEncoded(certificateEncoded);
    }

    private static String getCertificateEncoded(final Certificate certificate) throws Exception {
        final byte[] certificateEncoded = certificate.getEncoded();
        final String encoded = ENCODER.encodeToString(certificateEncoded);
        return String.format(CERTIFICATE_FORMAT, encoded);
    }

    private static String getKeyEncoded(final Key key) {
        final byte[] keyEncoded = key.getEncoded();
        final String encoded = ENCODER.encodeToString(keyEncoded);
        return String.format(KEY_FORMAT, encoded);
    }

    private static Path writeCertificateEncoded(final String certificateEncoded) throws IOException {
        final Path certificateFile = Files.createTempFile(TestCassandraCQLExecutionServiceV3.class.getSimpleName(), ".crt");
        Files.write(certificateFile, certificateEncoded.getBytes(StandardCharsets.UTF_8));
        certificateFile.toFile().deleteOnExit();
        return certificateFile;
    }
}
