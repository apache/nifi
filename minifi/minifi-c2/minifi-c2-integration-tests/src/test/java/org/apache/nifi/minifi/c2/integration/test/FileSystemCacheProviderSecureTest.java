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

package org.apache.nifi.minifi.c2.integration.test;

import com.palantir.docker.compose.DockerComposeRule;
import org.apache.nifi.minifi.c2.integration.test.health.HttpsStatusCodeHealthCheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class FileSystemCacheProviderSecureTest extends AbstractTestSecure {
    private static SSLSocketFactory healthCheckSocketFactory;

    // Not annotated as rule because we need to generate certificatesDirectory first
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("target/test-classes/docker-compose-FileSystemCacheProviderSecureTest.yml")
            .waitingForServices(Arrays.asList("squid", "c2"),
                    new HttpsStatusCodeHealthCheck(container -> C2_URL, containers -> containers.get(0), containers -> containers.get(1), () -> healthCheckSocketFactory, 403))
            .build();
    private static Path certificatesDirectory;
    private static SSLContext trustSslContext;

    public FileSystemCacheProviderSecureTest() {
        super(docker, certificatesDirectory, trustSslContext);
    }

    /**
     * Generates certificates with the tls-toolkit and then starts up the docker compose file
     */
    @BeforeClass
    public static void initCertificates() throws Exception {
        certificatesDirectory = Paths.get(FileSystemCacheProviderSecureTest.class.getClassLoader()
                .getResource("docker-compose-FileSystemCacheProviderSecureTest.yml").getFile()).getParent().toAbsolutePath().resolve("certificates-FileSystemCacheProviderSecureTest");
        trustSslContext = initCertificates(certificatesDirectory, Arrays.asList("c2"));
        healthCheckSocketFactory = trustSslContext.getSocketFactory();
        docker.before();
    }

    @AfterClass
    public static void cleanup() {
        docker.after();
    }

    @Before
    public void setup() {
        super.setup(docker);
    }
}
