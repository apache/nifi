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

import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import org.apache.nifi.minifi.integration.util.LogUtil;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Timeout(60)
public class C2ProtocolIntegrationTest {
    private static final String AGENT_1 = "minifi-edge1";
    private static final String AGENT_2 = "minifi-edge2";
    private static final String AGENT_3 = "minifi-edge3";
    private static final String AGENT_CLASS_1 = "raspi3";
    private static final String AGENT_CLASS_2 = "raspi4";
    private static final String SERVICE = "c2-authoritative";
    private static final String CONFIG_YAML = "config.text.yml.v2";
    private static Path certificatesDirectory;
    private static SSLContext trustSslContext;
    private static SSLSocketFactory healthCheckSocketFactory;
    public static DockerComposeExtension docker = DockerComposeExtension.builder()
            .file("target/test-classes/docker-compose-c2-protocol.yml")
            .waitingForService(AGENT_1, HealthChecks.toRespond2xxOverHttp(8000, dockerPort -> "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort()))
            .waitingForService(AGENT_2, HealthChecks.toRespond2xxOverHttp(8000, dockerPort -> "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort()))
            .waitingForService(AGENT_3, HealthChecks.toRespond2xxOverHttp(8000, dockerPort -> "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort()))
            .build();

    private static Path resourceDirectory;
    private static Path authoritativeFiles;
    private static Path minifiEdge1Version2;
    private static Path minifiEdge2Version2;
    private static Path minifiEdge3Version2;

    /**
     * Generates certificates with the tls-toolkit and then starts up the docker compose file
     */
    @BeforeAll
    public static void init() throws Exception {
        resourceDirectory = Paths.get(C2ProtocolIntegrationTest.class.getClassLoader()
                .getResource("docker-compose-c2-protocol.yml").getFile()).getParent();
        certificatesDirectory = resourceDirectory.toAbsolutePath().resolve("certificates-c2-protocol");
        authoritativeFiles = resourceDirectory.resolve("c2").resolve("protocol").resolve(SERVICE).resolve("files");
        minifiEdge1Version2 = authoritativeFiles.resolve("edge1").resolve(AGENT_CLASS_1).resolve(CONFIG_YAML);
        minifiEdge2Version2 = authoritativeFiles.resolve("edge2").resolve(AGENT_CLASS_1).resolve(CONFIG_YAML);
        minifiEdge3Version2 = authoritativeFiles.resolve("edge3").resolve(AGENT_CLASS_2).resolve(CONFIG_YAML);

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
        for (String serverHostname : Arrays.asList(SERVICE, AGENT_1, AGENT_2, AGENT_3)) {
            toolkitCommandLine.add("-n");
            toolkitCommandLine.add(serverHostname);
        }
        Files.createDirectories(certificatesDirectory);
        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine();
        tlsToolkitStandaloneCommandLine.parse(toolkitCommandLine.toArray(new String[toolkitCommandLine.size()]));
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig());

        TlsConfiguration tlsConfiguration = new StandardTlsConfiguration(
                null,null,null,
                certificatesDirectory.resolve(SERVICE).resolve("truststore.jks").toFile().getAbsolutePath(),
                "badTrustPass",
                KeystoreType.JKS);
        trustSslContext = SslContextFactory.createSslContext(tlsConfiguration);
        healthCheckSocketFactory = trustSslContext.getSocketFactory();

        docker.before();
    }

    @AfterAll
    public static void stopDocker() {
        docker.after();
    }

    @Test
    public void testFlowPublishThroughC2Protocol() throws Exception {
        LogUtil.verifyLogEntries("c2/protocol/minifi-edge1/expected.json", docker.containers().container(AGENT_1));
        LogUtil.verifyLogEntries("c2/protocol/minifi-edge2/expected.json", docker.containers().container(AGENT_2));
        LogUtil.verifyLogEntries("c2/protocol/minifi-edge3/expected.json", docker.containers().container(AGENT_3));
    }
}
