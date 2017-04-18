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

package org.apache.nifi.minifi.integration.c2;

import com.palantir.docker.compose.DockerComposeRule;
import org.apache.nifi.minifi.c2.integration.test.health.HttpsStatusCodeHealthCheck;
import org.apache.nifi.minifi.integration.util.LogUtil;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HierarchicalC2IntegrationTest {
    private static Path certificatesDirectory;
    private static SSLContext trustSslContext;
    private static SSLSocketFactory healthCheckSocketFactory;

    // Not annotated as rule because we need to generate certificatesDirectory first
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("target/test-classes/docker-compose-c2-hierarchical.yml")
            .waitingForServices(Arrays.asList("squid-edge3", "c2"),
                    new HttpsStatusCodeHealthCheck(container -> "https://c2-authoritative:10443/c2/config",
                            containers -> containers.get(0), containers -> containers.get(1), () -> healthCheckSocketFactory, 403))
            .build();
    private static Path resourceDirectory;
    private static Path authoritativeFiles;
    private static Path minifiEdge1Version2;
    private static Path minifiEdge2Version2;
    private static Path minifiEdge3Version2;

    /**
     * Generates certificates with the tls-toolkit and then starts up the docker compose file
     */
    @BeforeClass
    public static void initCertificates() throws Exception {
        resourceDirectory = Paths.get(HierarchicalC2IntegrationTest.class.getClassLoader()
                .getResource("docker-compose-c2-hierarchical.yml").getFile()).getParent();
        certificatesDirectory = resourceDirectory.toAbsolutePath().resolve("certificates-c2-hierarchical");
        authoritativeFiles = resourceDirectory.resolve("c2").resolve("hierarchical").resolve("c2-authoritative").resolve("files");
        minifiEdge1Version2 = authoritativeFiles.resolve("edge1").resolve("raspi3").resolve("config.text.yml.v2");
        minifiEdge2Version2 = authoritativeFiles.resolve("edge2").resolve("raspi2").resolve("config.text.yml.v2");
        minifiEdge3Version2 = authoritativeFiles.resolve("edge3").resolve("raspi3").resolve("config.text.yml.v2");

        if (Files.exists(minifiEdge1Version2)) {
            Files.delete(minifiEdge1Version2);
        }
        if (Files.exists(minifiEdge2Version2)) {
            Files.delete(minifiEdge2Version2);
        }
        if (Files.exists(minifiEdge3Version2)) {
            Files.delete(minifiEdge3Version2);
        }

        List<String> toolkitCommandLine = new ArrayList<>(Arrays.asList("-O", "-o", certificatesDirectory.toFile().getAbsolutePath(), "-S", "badKeystorePass", "-P", "badTrustPass"));
        for (String serverHostname : Arrays.asList("c2-authoritative", "minifi-edge1", "c2-edge2", "minifi-edge3")) {
            toolkitCommandLine.add("-n");
            toolkitCommandLine.add(serverHostname);
        }
        Files.createDirectories(certificatesDirectory);
        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine();
        tlsToolkitStandaloneCommandLine.parse(toolkitCommandLine.toArray(new String[toolkitCommandLine.size()]));
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig());

        trustSslContext = SslContextFactory.createTrustSslContext(certificatesDirectory.resolve("c2-authoritative")
                .resolve("truststore.jks").toFile().getAbsolutePath(), "badTrustPass".toCharArray(), "jks", "TLS");
        healthCheckSocketFactory = trustSslContext.getSocketFactory();

        docker.before();
    }

    @AfterClass
    public static void afterClass() {
        docker.after();
    }

    @Test(timeout = 120_000)
    public void testMiNiFiEdge1() throws Exception {
        LogUtil.verifyLogEntries("c2/hierarchical/minifi-edge1/expected.json", docker.containers().container("minifi-edge1"));
        Path csvToJsonDir = resourceDirectory.resolve("standalone").resolve("v1").resolve("CsvToJson").resolve("yml");
        Files.copy(csvToJsonDir.resolve("CsvToJson.yml"), minifiEdge1Version2);
        LogUtil.verifyLogEntries("standalone/v1/CsvToJson/yml/expected.json", docker.containers().container("minifi-edge1"));
    }

    @Test(timeout = 120_000)
    public void testMiNiFiEdge2() throws Exception {
        LogUtil.verifyLogEntries("c2/hierarchical/minifi-edge2/expected.json", docker.containers().container("minifi-edge2"));
        Path csvToJsonDir = resourceDirectory.resolve("standalone").resolve("v1").resolve("CsvToJson").resolve("yml");
        Files.copy(csvToJsonDir.resolve("CsvToJson.yml"), minifiEdge2Version2);
        LogUtil.verifyLogEntries("standalone/v1/CsvToJson/yml/expected.json", docker.containers().container("minifi-edge2"));
    }

    @Test(timeout = 120_000)
    public void testMiNiFiEdge3() throws Exception {
        LogUtil.verifyLogEntries("c2/hierarchical/minifi-edge3/expected.json", docker.containers().container("minifi-edge3"));
        Path csvToJsonDir = resourceDirectory.resolve("standalone").resolve("v1").resolve("CsvToJson").resolve("yml");
        Files.copy(csvToJsonDir.resolve("CsvToJson.yml"), minifiEdge3Version2);
        LogUtil.verifyLogEntries("standalone/v1/CsvToJson/yml/expected.json", docker.containers().container("minifi-edge3"));
    }
}
