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

package org.apache.nifi.graph;

import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.graph.Neo4JCypherClientService.CONNECTION_URL;
import static org.apache.nifi.graph.Neo4JCypherClientService.PASSWORD;
import static org.apache.nifi.graph.Neo4JCypherClientService.SSL_TRUST_STORE_FILE;
import static org.apache.nifi.graph.Neo4JCypherClientService.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class ITNeo4JCypherClientServiceSSL {
    private static final String ADMIN_USER = "neo4j";

    private static final String ADMIN_ACCESS = UUID.randomUUID().toString();

    private static final String IMAGE_NAME = System.getProperty("neo4j.docker.image");

    private static final Map<String, String> CONTAINER_ENVIRONMENT = new LinkedHashMap<>();

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    private static final String CERTIFICATE_FORMAT = "-----BEGIN CERTIFICATE-----%n%s%n-----END CERTIFICATE-----";

    private static final String KEY_FORMAT = "-----BEGIN PRIVATE KEY-----%n%s%n-----END PRIVATE KEY-----";

    private static final String CLIENT_AUTHENTICATION = "NONE";

    private static final String SSL_DIRECTORY = "/ssl";

    private static final String CERTIFICATE_FILE = "public.crt";

    private static final String CONTAINER_CERTIFICATE_PATH = String.format("%s/%s", SSL_DIRECTORY, CERTIFICATE_FILE);

    private static final String KEY_FILE = "private.key";

    private static final String CONTAINER_KEY_PATH = String.format("%s/%s", SSL_DIRECTORY, KEY_FILE);

    private static Neo4jContainer<?> container;

    private static String trustStoreFilePath;

    private GraphClientService clientService;

    @BeforeAll
    public static void setContainerEnvironment() throws Exception {
        container = new Neo4jContainer<>(DockerImageName.parse(IMAGE_NAME)).withAdminPassword(ADMIN_ACCESS);
        setCertificatePrivateKey();

        // Set Neo4j Environment Variables based on https://neo4j.com/developer/kb/setting-up-ssl-with-docker/
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_bolt_enabled", Boolean.TRUE.toString());
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_bolt_client__auth", CLIENT_AUTHENTICATION);
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_bolt_base__directory", SSL_DIRECTORY);
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_bolt_private__key", KEY_FILE);
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_bolt_public__certificate", CERTIFICATE_FILE);

        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_connector_bolt_tls__level", "REQUIRED");
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_connector_https_enabled", Boolean.TRUE.toString());

        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_https_enabled", Boolean.TRUE.toString());
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_https_client__auth", CLIENT_AUTHENTICATION);
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_https_base__directory", SSL_DIRECTORY);
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_https_private__key", KEY_FILE);
        CONTAINER_ENVIRONMENT.put("NEO4J_dbms_ssl_policy_https_public__certificate", CERTIFICATE_FILE);

        container.withEnv(CONTAINER_ENVIRONMENT);
        container.start();
    }

    @BeforeEach
    public void setUp() throws Exception {
        final String boltUrl = container.getBoltUrl();

        clientService = new Neo4JCypherClientService();
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(Neo4JCypherClientService.class.getSimpleName(), clientService);
        runner.setProperty(clientService, CONNECTION_URL, boltUrl);
        runner.setProperty(clientService, USERNAME, ADMIN_USER);
        runner.setProperty(clientService, PASSWORD, ADMIN_ACCESS);
        runner.setProperty(clientService, SSL_TRUST_STORE_FILE, trustStoreFilePath);
        runner.enableControllerService(clientService);
    }

    @Test
    public void testQuery() {
        String query = "create (n { name:'abc' }) return n.name";

        final List<Map<String, Object>> result = new ArrayList<>();
        Map<String, String> attributes = clientService.executeQuery(query, new HashMap<>(), (record, hasMore) -> result.add(record));
        assertEquals("0",attributes.get(GraphClientService.LABELS_ADDED));
        assertEquals("1",attributes.get(GraphClientService.NODES_CREATED));
        assertEquals("0",attributes.get(GraphClientService.NODES_DELETED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_CREATED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_DELETED));
        assertEquals("1",attributes.get(GraphClientService.PROPERTIES_SET));
        assertEquals("1",attributes.get(GraphClientService.ROWS_RETURNED));
        assertEquals(1, result.size());
        assertEquals("abc", result.get(0).get("n.name"));
    }

    private static void setCertificatePrivateKey() throws Exception {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        final KeyStore keyStore = KeyStoreUtils.loadKeyStore(
                tlsConfiguration.getKeystorePath(),
                tlsConfiguration.getKeystorePassword().toCharArray(),
                tlsConfiguration.getKeystoreType().getType()
        );

        final String alias = keyStore.aliases().nextElement();
        final Key key = keyStore.getKey(alias, tlsConfiguration.getKeyPassword().toCharArray());
        final String keyEncoded = getKeyEncoded(key);
        container.withCopyToContainer(Transferable.of(keyEncoded), CONTAINER_KEY_PATH);

        final Certificate certificate = keyStore.getCertificate(alias);
        final String certificateEncoded = getCertificateEncoded(certificate);
        container.withCopyToContainer(Transferable.of(certificateEncoded), CONTAINER_CERTIFICATE_PATH);
        final Path certificateFilePath = writeCertificateEncoded(certificateEncoded);
        trustStoreFilePath = certificateFilePath.toString();
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
        final Path certificateFile = Files.createTempFile(ITNeo4JCypherClientServiceSSL.class.getSimpleName(), ".crt");
        Files.write(certificateFile, certificateEncoded.getBytes(StandardCharsets.UTF_8));
        certificateFile.toFile().deleteOnExit();
        return certificateFile;
    }
}
